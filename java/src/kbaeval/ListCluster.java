package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.type.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class ListCluster {
   public static final Log log = new Log( ListCluster.class );
   HashMap<String, ArrayList<MatchesWritable>> matches;
   ArrayMap3<NodeWritable, Integer, String> results;

   public ListCluster(String matchesfile, String resultsfile) {
       matches = readMatches(matchesfile);
       results = readResults(resultsfile);
       setRelevanceResults();
   }
   
   public HashMap<String, ArrayList<MatchesWritable>> readMatches(String matchesfile) {
       Datafile df = new Datafile(matchesfile);
       MatchesFile mf = new MatchesFile(df);
       return mf.getMap();
   }
   
   public ArrayMap3<NodeWritable, Integer, String> readResults(String resultsfile) {
       ArrayMap3<NodeWritable, Integer, String> result = new ArrayMap3();
       Datafile df = new Datafile(HDFSPath.getFS(), resultsfile);
       ClusterFile tf = new ClusterFile(df);
       for (ClusterWritable t : tf) {
           for (NodeWritable u : t.nodes) {
              result.add(u, t.clusterid, "");
           }
       }
       return result;
   }
   
   public void setRelevanceResults() {
       for (Map.Entry<NodeWritable, Tuple2<Integer, String>> entry : results) {
           NodeWritable update = entry.getKey();
           String updateid = sprintf("%s-%d", update.docid, update.sentenceNumber);
           ArrayList<MatchesWritable> nuggets = matches.get(updateid);
           if (nuggets != null)
              entry.setValue(new Tuple2<Integer, String>(entry.getValue().key, listNuggets(nuggets)));
       }
   }
   
   public void listRelevantResults() {
       for (Map.Entry<NodeWritable, Tuple2<Integer, String>> entry : results) {
           NodeWritable update = entry.getKey();
           String nuggets = entry.getValue().value;
           int cluster = entry.getValue().key;
           if (nuggets.length() > 0) {
               log.printf("%6d %s-%d %s", cluster, update.docid, update.sentenceNumber, nuggets);
           }
       }
   }
   
   public String listNuggets(ArrayList<MatchesWritable> nuggets) {
       StringBuilder sb = new StringBuilder();
       for (MatchesWritable match : nuggets) {
           sb.append(",").append(match.nuggetid);
       }
       return sb.deleteCharAt(0).toString();
   }
   
    public static void main(String[] args) {
        Conf conf = new Conf(args, "-m matches -r results");
        ListCluster relevance = new ListCluster(conf.get("matches"), conf.get("results"));
        relevance.listRelevantResults();
    }
}
