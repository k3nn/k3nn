package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Url;
import KNN2.Stream;
import KNN2.UrlS;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class TryCluster {

    public static final Log log = new Log(TryCluster.class);
    Stream<UrlS> stream = new Stream();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    Cluster cluster;
    Conf conf;

    public TryCluster(Conf conf, String yesterday, String today, int clusterid) {
        this.conf = conf;
        Cluster<UrlS> end = readResults(today, clusterid);
        stream = new Stream();
        stream.setNextClusterID(clusterid);
        Cluster<UrlS> start = null;
        if (yesterday != null && yesterday.length() > 0) {
            start = readResults(yesterday, clusterid);
        }
        ArrayMap<Long, UrlS> newurls = new ArrayMap();
        for (UrlS url : end.getUrls()) {
            newurls.add(url.getCreationTime(), url);
        }
        if (start != null) {
            log.info("%s", start.evalall());
        }
        LOOP:
        for (Url url : newurls.ascending().values()) {
            if (start != null) {
                for (UrlS u : start.getUrls()) {
                    if (u.getID() == url.getID()) {
                        continue LOOP;
                    }
                }
            }
            UrlS u = new UrlS(url.getID(), url.getDomain(), url.getTitle(), url.getFeatures(), url.getCreationTime());
            stream.urls.put(u.getID(), u);
            stream.add(u, u.getFeatures());
            if (start == null) {
                start = stream.getCluster(clusterid);
            }
            log.info("add %d", u.getID());
            if (start != null) {
                log.info("%s", start.evalall());
            }
        }
        
    }

    public Cluster readResults(String resultsfile, int clusterid) {
        Datafile df = new Datafile(conf, resultsfile);
        ClusterFile tf = new ClusterFile(df);
        ArrayList<ClusterWritable> list = new ArrayList();
        for (ClusterWritable t : tf) {
            //log.info("row %d", t.clusterid);
            if (t.clusterid == clusterid) {
                list.add(t);
            }
        }
        return select(list);
    }

    public Cluster select(ArrayList<ClusterWritable> list) {
        for (ClusterWritable r : list) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(r.title));
            UrlS u = new UrlS(r.urlid, r.domain, r.title, features, r.creationtime);
            stream.urls.put(u.getID(), u);
            stream.iiurls.add(u, features);
        }
        Cluster c = stream.createCluster(list.get(0).clusterid);
//        for (Map.Entry<Integer, UrlM> u : stream.urls.entrySet())
//            log.info("%d %s", u.getKey(), u.getValue());
        for (ClusterWritable r : list) {
            Url url = stream.urls.get(r.urlid);
            //log.info("%d %s", r.urlid, url);
            ArrayList<Integer> nn = getNN(r.nnid);
            ArrayList<Double> score = getScores(r.nnscore);
            for (int i = 0; i < nn.size(); i++) {
                Url u = stream.urls.get(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                url.add(e);
            }
            url.setCluster(c);
        }
        c.setBase(Cluster.getBase(c.getUrls()));
        return c;
    }

    public ArrayList<Integer> getNN(String nn) {
        ArrayList<Integer> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Integer.parseInt(n));
        }
        return result;
    }

    public ArrayList<Double> getScores(String nn) {
        ArrayList<Double> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Double.parseDouble(n));
        }
        return result;
    }

    public static void main(String[] args) {
        Conf conf = new Conf(args, "-y [yesterday] -t today -c clusterid");
        TryCluster relevance = new TryCluster(conf, conf.get("yesterday"), conf.get("today"), conf.getInt("clusterid", 0));
    }
}
