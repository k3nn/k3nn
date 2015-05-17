package kbaeval;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Node;
import KNN.Stream;
import KNN.NodeS;
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
public class TryClusterOldHD {

    public static final Log log = new Log(TryClusterOldHD.class);
    Stream<NodeS> stream = new Stream();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    Cluster cluster;
    Conf conf;

    public TryClusterOldHD(Conf conf, String yesterday, String today, int clusterid) {
        this.conf = conf;
        Cluster<NodeS> end = readResults(today, clusterid);
        stream = new Stream();
        stream.setNextClusterID(clusterid);
        Cluster<NodeS> start = null;
        if (yesterday != null && yesterday.length() > 0)
            start = readResults(yesterday, clusterid);
        ArrayMap<Long, NodeS> newurls = new ArrayMap();
        for (NodeS url : end.getNodes()) {
            newurls.add(url.getCreationTime(), url);
        }
        if (start != null) {
            log.info("%s", start.evalall());
        }
        LOOP:
        for (Node url : newurls.ascending().values()) {
            if (start != null) {
                for (NodeS u : start.getNodes()) {
                    if (u.getID() == url.getID()) {
                        continue LOOP;
                    }
                }
            } else {
                stream.setNextClusterID(clusterid);
            }
            NodeS u = new NodeS(url.getID(), url.getDomain(), url.getContent(), url.getTerms(), url.getCreationTime());
            stream.nodes.put(u.getID(), u);
            stream.add(u, u.getTerms());
            log.info("add %d", u.getID());
            if (stream.getCluster(clusterid) != null)
               log.info("%s", stream.getCluster(clusterid).evalall());
        }
    }
    
    public Cluster readResults(String resultsfile, int clusterid) {
        Datafile df = new Datafile(conf, resultsfile);
        ClusterNodeFile tf = new ClusterNodeFile(df);
        ArrayList<ClusterNodeWritable> list = new ArrayList();
        for (ClusterNodeWritable t : tf) {
            //log.info("row %d", t.clusterid);
            if (t.clusterID == clusterid) {
                list.add(t);
            }
        }
        return select(list);
    }

    public Cluster select(ArrayList<ClusterNodeWritable> list) {
        for (ClusterNodeWritable r : list) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(r.content));
            NodeS u = new NodeS(r.sentenceID, r.domain, r.content, features, r.creationTime);
            stream.nodes.put(u.getID(), u);
            stream.iinodes.add(u, features);
        }
        if (list.size() == 0) {
            return null;
        }
        Cluster c = stream.createCluster(list.get(0).clusterID);
//        for (Map.Entry<Integer, UrlM> u : stream.urls.entrySet())
//            log.info("%d %s", u.getKey(), u.getValue());
        for (ClusterNodeWritable r : list) {
            Node url = stream.nodes.get(r.sentenceID);
            //log.info("%d %s", r.urlid, url);
            ArrayList<Integer> nn = getNN(r.nnid);
            ArrayList<Double> score = getScores(r.nnscore);
            for (int i = 0; i < nn.size(); i++) {
                Node u = stream.nodes.get(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                url.add(e);
            }
            url.setCluster(c);
        }
        c.setBase(Cluster.getBase(c.getNodes()));
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
        TryClusterOldHD relevance = new TryClusterOldHD(conf, conf.get("yesterday"), conf.get("today"), conf.getInt("clusterid", 0));
    }
}
