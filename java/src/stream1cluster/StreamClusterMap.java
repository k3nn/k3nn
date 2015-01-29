package stream1cluster;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Url;
import KNN.UrlClusterListener;
import KNN.UrlM;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class StreamClusterMap extends Mapper<LongWritable, SentenceWritable, IntWritable, StreamClusterWritable> implements UrlClusterListener {

    public static final Log log = new Log(StreamClusterMap.class);
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    
    // Map<TopicID, List<SentenceID that contain all query terms>>
    HashMap<Integer, ArrayList<Integer>> topicSentences;
    ArrayMap<IntWritable, StreamClusterWritable> out = new ArrayMap();
    HashMap<Integer, HashSet<Integer>> outclusters = new HashMap();
    Configuration conf;
    String inputfile;
    Stream<UrlM> stream;
    String todayString;
    Date today;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        topicSentences = StreamClusterJob.getTopicSentences(conf);
        stream = new Stream();
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        inputfile = ((FileSplit) context.getInputSplit()).getPath().toString();
        todayString = filename.substring(filename.lastIndexOf('/') + 1);
        try {
            today = DateTools.FORMAT.Y_M_D.parse(todayString);
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
        }
        readClusters();
        stream.setListenr(stream.urls, topicSentences.keySet(), this);
    }

    @Override
    public void urlChanged(Url addedurl, HashSet<Url> urls) {
        Cluster<UrlM> c = addedurl.getCluster();
        if (c != null) {
            HashSet<Integer> reducers = new HashSet();
            for (Url url : c.getUrls()) {
                log.info("urlChanged %d %d", url.getID(), url.getCluster().getID());
                Collection<Integer> urlreducers = this.topicSentences.get(url.getID());
                if (urlreducers != null) {
                    reducers.addAll(urlreducers);
                }
            }
            for (int reducer : reducers) {
                out(reducer, addedurl, c);
            }
        }
    }

    public void out(int reducer, Url addedurl, Cluster<UrlM> cluster) {
        StreamClusterWritable w = new StreamClusterWritable();
        w.clusterid = cluster.getID();
        for (Url url : cluster.getUrls()) {
            if (url != addedurl) {
                w.urls.add(toUrlWritable(url));
            }
        }
        w.urls.add(toUrlWritable(addedurl));
        IntWritable key = new IntWritable(reducer);
        out.add(key, w);
    }

    public UrlWritable toUrlWritable(Url url) {
        UrlWritable u = new UrlWritable();
        u.creationtime = url.getCreationTime();
        u.domain = url.getDomain();
        u.nnid = url.getNN();
        u.nnscore = url.getScore();
        u.urlid = url.getID();
        return u;
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        ArrayList<String> features = tokenizer.tokenize(value.sentence);
        try {
            if (features.size() > 1) {
                HashSet<String> uniq = new HashSet(features);
                UrlM url = new UrlM(value.id, value.domain, value.creationtime, uniq);
                UrlM exists = stream.urls.get(url.getID());
                if (exists != null) {
                    log.info("existing %d %s", url.getID(), value.creationtime, value.sentence);
                }
                stream.urls.put(url.getID(), url);
                stream.add(url, uniq);
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        stream = null;

        HashMap<Integer, String> titles = this.getTitlesYesterday();
        //log.info("titles %d", titles.size());
        titles.putAll(this.getTitlesToday());
        //log.info("titles %d", titles.size());
        for (Map.Entry<IntWritable, StreamClusterWritable> entry : out) {
            StreamClusterWritable w = entry.getValue();
            for (UrlWritable u : w.urls) {
                u.title = titles.get(u.urlid);
            }
            Collections.sort(w.urls, Sorter.instance);
            context.write(entry.getKey(), w);
        }
    }

    private static class Sorter implements Comparator<UrlWritable> {

        static Sorter instance = new Sorter();

        @Override
        public int compare(UrlWritable o1, UrlWritable o2) {
            return (int) (o1.creationtime - o2.creationtime);
        }

    }

    public void readClusters() throws IOException, IOException, IOException, IOException, IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("clusters"));
        Date previousdate = DateTools.daysBefore(today, 1);
        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
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
                    if (cw.clusterid >= 0) {
                        Cluster c = stream.getCluster(cw.clusterid);
                        if (c == null) {
                            c = stream.createCluster(cw.clusterid);
                        }
                        url.setCluster(c);
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
        HDFSPath dir = new HDFSPath(conf, conf.get("clusters"));
        Date previousdate = DateTools.daysBefore(today, 1);

        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        log.info("read clusters %b %s", df.exists(), df.getCanonicalPath());
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
        Datafile df = new Datafile(conf, inputfile);
        log.info("read clusters %b %s", df.exists(), df.getCanonicalPath());
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
