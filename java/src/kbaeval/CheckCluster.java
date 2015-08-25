package kbaeval;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Node;
import KNN.NodeS;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class CheckCluster {
   public static final Log log = new Log( CheckCluster.class );
   Stream<NodeS> stream = new Stream();
   DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
   Cluster cluster;

   public CheckCluster(String resultsfile, int clusterid) {
       cluster = readResults(resultsfile, clusterid);
       log.info("%s", cluster.evalall());
   }
   
   public Cluster readResults(String resultsfile, int clusterid) {
       Datafile df = new Datafile(HDFSPath.getFS(), resultsfile);
       ClusterNodeFile tf = new ClusterNodeFile(df);
       ArrayList<ClusterNodeWritable> list = new ArrayList();
       for (ClusterNodeWritable t : tf) {
           //log.info("row %d", t.clusterid);
           if (t.clusterID == clusterid)
               list.add(t);
       }
       return select(list);
   }
   
    public Cluster select(ArrayList<ClusterNodeWritable> list) {
        for (ClusterNodeWritable r : list) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(r.content));
            NodeS u = new NodeS(r.sentenceID, r.domain, r.content, features, r.creationTime);
            stream.nodes.put(u.getID(), u);
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
        Conf conf = new Conf(args, "-r results -c clusterid");
        CheckCluster relevance = new CheckCluster(conf.get("results"), conf.getInt("clusterid", 0));
    }
}
