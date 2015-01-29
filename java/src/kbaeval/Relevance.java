package kbaeval;

import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class Relevance {
   public static final Log log = new Log( Relevance.class );
   HashMap<String, ArrayList<MatchesWritable>> matches;
   ArrayMap<TrecWritable, String> results;

   public Relevance(String matchesfile, String resultsfile) {
       matches = readMatches(matchesfile);
       results = readResults(resultsfile);
       setRelevanceResults();
   }
   
   public HashMap<String, ArrayList<MatchesWritable>> readMatches(String matchesfile) {
       Datafile df = new Datafile(matchesfile);
       MatchesFile mf = new MatchesFile(df);
       return mf.getMap();
   }
   
   public ArrayMap<TrecWritable, String> readResults(String resultsfile) {
       ArrayMap<TrecWritable, String> result = new ArrayMap();
       Datafile df = new Datafile(resultsfile);
       TrecFile tf = new TrecFile(df);
       for (TrecWritable t : tf) {
           result.add(t, "");
       }
       return result;
   }
   
   public void setRelevanceResults() {
       for (Map.Entry<TrecWritable, String> entry : results) {
           TrecWritable update = entry.getKey();
           String updateid = sprintf("%s-%d", update.document, update.sentence);
           ArrayList<MatchesWritable> nuggets = matches.get(updateid);
           if (nuggets != null)
              entry.setValue(listNuggets(nuggets));
       }
   }
   
   public void listRelevantResults() {
       for (Map.Entry<TrecWritable, String> entry : results) {
           TrecWritable update = entry.getKey();
           String nuggets = entry.getValue();
           if (nuggets.length() > 0) {
               log.printf("%2d %s-%d %s", update.topic, update.document, update.sentence, nuggets);
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
        ArgsParser ap = new ArgsParser(args, "-m matches -r results");
        Relevance relevance = new Relevance(ap.get("matches"), ap.get("results"));
        relevance.listRelevantResults();
    }
}
