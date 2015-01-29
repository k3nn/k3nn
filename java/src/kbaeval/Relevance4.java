package kbaeval;

import RelCluster.RelClusterWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class Relevance4 extends Relevance2 {
   public static final Log log = new Log( Relevance4.class );
   ArrayMap<TrecWritable, String> results;

    public Relevance4(String matchesfile, String resultsfile) {
       matches = readMatches(matchesfile);
       results = readTrecResults(resultsfile);
       setRelevanceTrecResults(results);
    }
   
   @Override
   public void listRelevantTrecResults(ArrayMap<TrecWritable, String> results) {
       for (Map.Entry<TrecWritable, String> entry : results) {
           TrecWritable update = entry.getKey();
           String nuggets = entry.getValue();
           if (nuggets.length() > 0) {
               for (String nugget : nuggets.split(","))
                   matchednuggets.add(nugget);
           }
       }
       log.info("%d %d %f", matchednuggets.size(), results.size(), matchednuggets.size()/(double)results.size());
       log.info("RECALL %f", matchednuggets.size()/(double)this.nuggetCount());
    }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "-m matches -r results");
        Relevance4 relevance = new Relevance4(ap.get("matches"), ap.get("results"));
        relevance.listRelevantTrecResults(relevance.results);
    }
}
