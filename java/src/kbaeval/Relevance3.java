package kbaeval;

import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class Relevance3 extends Relevance2 {
   public static final Log log = new Log( Relevance3.class );

    public Relevance3(String matchesfile, String resultsfile) {
        super(matchesfile, resultsfile);
    }
   
   @Override
   public void listRelevantResults(ArrayMap<MatchingClusterNodeWritable, String> results) {
       for (Map.Entry<MatchingClusterNodeWritable, String> entry : results) {
           MatchingClusterNodeWritable update = entry.getKey();
           String nuggets = entry.getValue();
           if (nuggets.length() > 0) {
               for (String nugget : nuggets.split(","))
                   matchednuggets.add(nugget);
           }
       }
       log.info("%d %d %f", matchednuggets.size(), results.size(), matchednuggets.size()/(double)results.size());
   }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "-m matches -r results");
        Relevance3 relevance = new Relevance3(ap.get("matches"), ap.get("results"));
        relevance.listRelevantResults(relevance.results);
    }
}
