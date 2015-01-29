package kba5clusterr;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Url;
import KNN.UrlM;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterMap.class);
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    ClusterWritable clusterwritable = new ClusterWritable();
    Configuration conf;
    Stream<UrlM> stream;
    String todayString;
    Date today;
    long time3daysago;

    public enum LABEL {

        CORRECT, TOOSHORT
    };

    @Override
    public void setup(Context context) throws IOException {
        stream = new Stream();
        conf = context.getConfiguration();
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        todayString = filename.substring(filename.lastIndexOf('/') + 1);
        try {
            today = DateTools.FORMAT.Y_M_D.parse(todayString);
            time3daysago = DateTools.daysBefore(today, 3).getTime() / 1000;
            //log.info("3daysago %d today %d", time3daysago, today.getTime() / 1000);
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
        }
        readClusters();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        //log.info("%d %s", value.id, value.sentence);
        ArrayList<String> features = tokenizer.tokenize(value.sentence);
        //log.info("map %d", value.id);
        try {
            if (features.size() > 1) {
                HashSet<String> uniq = new HashSet(features);
                context.getCounter(LABEL.CORRECT).increment(1);
                UrlM url = new UrlM(value.id, value.domain, value.creationtime, uniq);
                if (url.watch) {
                    log.info("read url id %d domain %d %d", url.getID(), url.getDomain(), url.getCreationTime());
                }
                UrlM exists = stream.urls.get(url.getID());
                if (exists != null) {
                    log.info("existing %d %s", url.getID(), value.creationtime, value.sentence);
                }
                stream.urls.put(url.getID(), url);
                stream.add(url, uniq);
            } else {
                context.getCounter(LABEL.TOOSHORT).increment(1);
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
        //log.info("mapped");
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        HashMap<Integer, Url> out = new HashMap();
        for (Cluster cluster : stream.getClusters()) {
            outCluster(out, cluster);
        }
        addUnclustered(out);
        stream = null;

        ArrayMap<Integer, Url> sorted = new ArrayMap();
        for (Url url : out.values()) {
            Cluster c = url.getCluster();
            sorted.add(c == null ? -1 : c.getID(), url);
        }
        out = null;

        HashMap<Integer, String> titles = this.getTitlesYesterday();
        titles.putAll(this.getTitlesToday());
        //log.info("out records %d %d", out.size(), sorted.size());
        ClusterFile cf = new ClusterFile(new Datafile(conf, conf.get("output")));
        //log.info("out %s", conf.get("output"));
        cf.openWrite();
        for (Url url : sorted.ascending().values()) {
            //log.info("write %d", url.getID());
            clusterwritable.set(url);
            clusterwritable.title = titles.get(url.getID());
            clusterwritable.write(cf);
        }
        cf.closeWrite();
    }

    public void outCluster(HashMap<Integer, Url> out, Cluster<UrlM> cluster) throws IOException, InterruptedException {
        if (cluster.watch) {
            log.info("write cluster %d %d\n%s", cluster.getID(), cluster.size(), cluster.evalall());
        }
        //log.info("write %d %d", Cluster.watch, cluster.getID());
        for (Url checkurl : cluster.getUrls()) {
            if (cluster.watch) {
                log.info("write cluster url creationtime %d %d %b",
                        checkurl.getID(),
                        checkurl.getCreationTime(),
                        checkurl.getCreationTime() > time3daysago);
            }
            if (clusterInTime(cluster) || stream.getChangedCluster().contains(cluster.getID())) {
                addRec(out, new HashSet(cluster.getUrls()));
                break;
            }
        }
    }

    public void addUnclustered(HashMap<Integer, Url> out) {
        HashSet<Url> urls = new HashSet();
        for (UrlM url : stream.urls.values()) {
            if (url.watch) {
                log.info("addUnclustered %d clustered %b intime %b outcontains %b", url.getID(), url.isClustered(), urlInTime(url), out.containsKey(url.getID()));
            }
            if (!url.isClustered() && urlInTime(url) && !out.containsKey(url.getID())) {
                urls.add(url);
            }
        }
        addRec(out, urls);
    }

    public boolean clusterInTime(Cluster<UrlM> cluster) {
        for (Url checkurl : cluster.getUrls()) {
            if (checkurl.getCreationTime() > time3daysago) {
                return true;
            }
        }
        return false;
    }

    public boolean urlInTime(Url url) {
        return url.getCreationTime() > time3daysago;
    }

    public void addRec(HashMap<Integer, Url> map, HashSet<Url> toadd) {
        while (toadd.size() > 0) {
            for (Url u : toadd) {
                map.put(u.getID(), u);
                //log.info("add %d", u.getID());
            }
            HashSet<Url> newadd = new HashSet();
            for (Url u : toadd) {
                if (u.getCreationTime() > this.time3daysago || u.isClustered()) {
                    for (int e = 0; e < u.getEdges(); e++) {
                        Url nn = u.getNN(e).getUrl();
                        if (nn != null && !map.containsKey(nn.getID())) {
                            newadd.add(nn);
                            //log.info("newadd %d", nn.getID());
                        }
                    }
                }
            }
            toadd = newadd;
        }
    }

    public void readClusters() throws IOException, IOException, IOException, IOException, IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("output")).getParent();
        Date previousdate = DateTools.daysBefore(today, 1);
        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        //log.info("read clusters %b %s", df.exists(), df.getCanonicalPath());
        if (df.exists() && df.getLength() > 0) {
            ArrayMap3<Integer, ArrayList<Integer>, ArrayList<Double>> nn = new ArrayMap3();
            ClusterFile cf = new ClusterFile(df);
            for (ClusterWritable cw : cf) {
                UrlM url = (UrlM) stream.urls.get(cw.urlid);
                if (url == null) {
                    url = getUrl(cw.urlid, cw.domain, cw.creationtime, cw.title);
                    if (url == null) {
                        log.info("NULL Url %d", cw.urlid);
                    }
                    nn.add(url.getID(), cw.getNN(), cw.getNNScore());
                    //log.info("readClusters url clusterid %d urlid %d nn %s score %s",
                    //        cw.clusterid, url.getID(), cw.getNN(), cw.getNNScore());
                    if (cw.clusterid >= 0) {
                        Cluster c = stream.getCluster(cw.clusterid);
                        if (c == null) {
                            c = stream.createCluster(cw.clusterid);
                        }
                        url.setCluster(c);
                        //if (c.getID() == Cluster.watch) {
                        //}
                    }
                } else {
                    log.info("readCluster exists %d", cw.urlid);
                }
            }
            for (Map.Entry<Integer, Tuple2<ArrayList<Integer>, ArrayList<Double>>> entry : nn) {
                UrlM url = stream.urls.get(entry.getKey());
                for (int i = 0; i < entry.getValue().value1.size(); i++) {
                    int nnid = entry.getValue().value1.get(i);
                    UrlM nnurl = stream.urls.get(nnid);
                    if (nnurl == null) {
                        log.info("Unknown url %d", nnid);
                    }
                    Edge e = new Edge(nnurl, entry.getValue().value2.get(i));
                    url.add(e);
                }
            }
            log.info("read %d", stream.urls.size());
        } else {
            stream.setStartClusterID(findNextCluster(dir, today));
        }

        ArrayList<Cluster> clusters = new ArrayList(stream.getClusters());
        for (Cluster c : clusters) {
            c.recheckBase();
        }
        stream.resetChangedCluster();
    }

    public static int findNextCluster(HDFSPath dir, Date today) throws IOException {
        ArrayList<String> filenames = dir.getFilenames();
        String name = null;
        for (String d : filenames) {
            try {
                if (DateTools.FORMAT.Y_M_D.parse(d).before(today)) {
                    name = d;
                }
            } catch (ParseException ex) {
                log.exception(ex, "findNextCluster invalid date %s", d);
            }
        }
        int clusterid = -1;
        if (name != null) {
            Datafile df = dir.getFile(name);
            ClusterFile cf = new ClusterFile(df);
            for (ClusterWritable u : cf) {
                clusterid = Math.max(clusterid, u.clusterid);
            }
        }
        return clusterid + 1;
    }

    public HashMap<Integer, String> getTitlesYesterday() {
        HashMap<Integer, String> result = new HashMap();
        HDFSPath dir = new HDFSPath(conf, conf.get("output")).getParent();
        Date previousdate = DateTools.daysBefore(today, 1);

        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        //log.info("read clusters %b %s", df.exists(), df.getCanonicalPath());
        if (df.exists()) {
            ClusterFile cf = new ClusterFile(df);
            for (ClusterWritable cw : cf) {
                result.put(cw.urlid, cw.title);
            }

        }
        return result;
    }

    public HashMap<Integer, String> getTitlesToday() {
        HashMap<Integer, String> result = new HashMap();
        HDFSPath dir = new HDFSPath(conf, conf.get("input")).getParent();

        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(today));
        if (df.exists()) {
            SentenceFile cf = new SentenceFile(df);
            for (SentenceWritable cw : cf) {
                result.put(cw.id, cw.sentence);
            }
        }
        return result;
    }

    public UrlM getUrl(int urlid, int domain, long creationtime, String title) {
        UrlM url = (UrlM) stream.urls.get(urlid);
        if (url == null) {
            ArrayList<String> features = tokenizer.tokenize(title);
            if (features.size() > 1) {
                //log.info("add url %d", urlid);
                url = new UrlM(urlid, domain, creationtime, features);
                stream.urls.put(url.getID(), url);
                stream.iiurls.add(url, features);
            }
        } else {
            log.info("getUrl() exists %d", urlid);
        }
        return url;
    }
}
