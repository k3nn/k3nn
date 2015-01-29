package kbaeval;

import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class Relevance2 {
   public static final Log log = new Log( Relevance2.class );
   HashMap<String, ArrayList<MatchesWritable>> matches;
   ArrayMap<RelClusterWritable, String> results;
   HashSet<String> matchednuggets = new HashSet();

   protected Relevance2() {}
   
   public Relevance2(String matchesfile, String resultsfile) {
       matches = readMatches(matchesfile);
       results = readResults(resultsfile);
       setRelevanceResults(results);
   }
   
   public HashMap<String, ArrayList<MatchesWritable>> readMatches(String matchesfile) {
       Datafile df = new Datafile(matchesfile);
       MatchesFile mf = new MatchesFile(df);
       return mf.getMap();
   }
   
   public int nuggetCount() {
       HashSet<String> nuggets = new HashSet();
       for (ArrayList<MatchesWritable> list : matches.values()) {
           for (MatchesWritable w : list) {
               nuggets.add(w.nuggetid);
           }
       }
       return nuggets.size();
   }
   
   public ArrayMap<RelClusterWritable, String> readResults(String resultsfile) {
       ArrayMap<RelClusterWritable, String> result = new ArrayMap();
       Datafile df = new Datafile(resultsfile);
       RelClusterFile tf = new RelClusterFile(df);
       for (RelClusterWritable t : tf) {
           result.add(t, "");
       }
       return result;
   }
   
   public ArrayMap<TrecWritable, String> readTrecResults(String resultsfile) {
       ArrayMap<TrecWritable, String> result = new ArrayMap();
       Datafile df = new Datafile(resultsfile);
       TrecFile tf = new TrecFile(df);
       for (TrecWritable t : tf) {
           result.add(t, "");
       }
       return result;
   }
   
   public void setRelevanceResults(ArrayMap<RelClusterWritable, String> results) {
       for (Map.Entry<RelClusterWritable, String> entry : results) {
           RelClusterWritable update = entry.getKey();
           String updateid = sprintf("%s-%d", update.documentid, update.row);
           ArrayList<MatchesWritable> nuggets = matches.get(updateid);
           if (nuggets != null)
              entry.setValue(listNuggets(nuggets));
       }
   }
   
   public void setRelevanceTrecResults(ArrayMap<TrecWritable, String> results) {
       for (Map.Entry<TrecWritable, String> entry : results) {
           TrecWritable update = entry.getKey();
           String updateid = sprintf("%s-%d", update.document, update.sentence);
           ArrayList<MatchesWritable> nuggets = matches.get(updateid);
           if (nuggets != null)
              entry.setValue(listNuggets(nuggets));
       }
   }
   
   public void listRelevantResults(ArrayMap<RelClusterWritable, String> results) {
       for (Map.Entry<RelClusterWritable, String> entry : results) {
           RelClusterWritable update = entry.getKey();
           String nuggets = entry.getValue();
           if (nuggets.length() > 0) {
               log.printf("\n%s", nuggets);
               for (String nugget : nuggets.split(","))
                   matchednuggets.add(nugget);
           }
           log.printf("%2d %s-%d %s", update.clusterid, update.documentid, update.row, update.title);
           
       }
       log.info("%d %d %f", matchednuggets.size(), results.size(), matchednuggets.size()/(double)results.size());
   }
   
   public void listRelevantTrecResults(ArrayMap<TrecWritable, String> results) {
       for (Map.Entry<TrecWritable, String> entry : results) {
           TrecWritable update = entry.getKey();
           String nuggets = entry.getValue();
           if (nuggets.length() > 0) {
               log.printf("\n%s", nuggets);
               for (String nugget : nuggets.split(","))
                   matchednuggets.add(nugget);
           }
           log.printf("%2d %s-%d", update.topic, update.document, update.sentence);
           
       }
       log.info("%d %d %f", matchednuggets.size(), results.size(), matchednuggets.size()/(double)results.size());
   }
   
   public String listNuggets(ArrayList<MatchesWritable> nuggets) {
       StringBuilder sb = new StringBuilder();
       for (MatchesWritable match : nuggets) {
           sb.append(",").append(match.nuggetid);
       }
       return sb.deleteCharAt(0).toString();
   }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "-m matches -r results");
        Relevance2 relevance = new Relevance2(ap.get("matches"), ap.get("results"));
        relevance.listRelevantResults(relevance.results);
    }
}
