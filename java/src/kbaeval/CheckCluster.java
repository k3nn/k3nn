package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN2.Stream;
import KNN.Url;
import KNN2.UrlS;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class CheckCluster {
   public static final Log log = new Log( CheckCluster.class );
   Stream<UrlS> stream = new Stream();
   DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
   Cluster cluster;

   public CheckCluster(String resultsfile, int clusterid) {
       cluster = readResults(resultsfile, clusterid);
       log.info("%s", cluster.evalall());
   }
   
   public Cluster readResults(String resultsfile, int clusterid) {
       Datafile df = new Datafile(HDFSPath.getFS(), resultsfile);
       ClusterFile tf = new ClusterFile(df);
       ArrayList<ClusterWritable> list = new ArrayList();
       for (ClusterWritable t : tf) {
           //log.info("row %d", t.clusterid);
           if (t.clusterid == clusterid)
               list.add(t);
       }
       return select(list);
   }
   
    public Cluster select(ArrayList<ClusterWritable> list) {
        for (ClusterWritable r : list) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(r.title));
            UrlS u = new UrlS(r.urlid, r.domain, r.title, features, r.creationtime);
            stream.urls.put(u.getID(), u);
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
        Conf conf = new Conf(args, "-r results -c clusterid");
        CheckCluster relevance = new CheckCluster(conf.get("results"), conf.getInt("clusterid", 0));
    }
}
