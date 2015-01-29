package kbaeval;

import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.type.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
/**
 *
 * @author jeroen
 */
public class PercInGT {
   public static final Log log = new Log( PercInGT.class );
   Set<String> matches;
   HashSet<String> results;

   public PercInGT(String matchesfile, String resultsfile) {
       matches = readMatches(matchesfile);
       results = readResults(resultsfile);
   }
   
   public Set<String> readMatches(String matchesfile) {
       Datafile df = new Datafile(matchesfile);
       MatchesFile mf = new MatchesFile(df);
       return mf.getMap().keySet();
   }
   
   public HashSet<String> readResults(String resultsfile) {
       HashSet<String> result = new HashSet();
       Datafile df = new Datafile(HDFSPath.getFS(), resultsfile);
       df.setBufferSize(100000000);
       StreamClusterFile tf = new StreamClusterFile(df);
       for (StreamClusterWritable t : tf) {
           for (UrlWritable u : t.urls) {
              String updateid = sprintf("%s-%d", u.docid, u.row);
              result.add(updateid);
           }
       }
       return result;
   }
   
   public int countExists() {
       int count = 0;
       for (String updateid : results) {
           if (matches.contains(updateid))
               count++;
       }
       return count;
   }
   
    public static void main(String[] args) {
        Conf conf = new Conf(args, "-m matches -r results");
        PercInGT relevance = new PercInGT(conf.get("matches"), conf.get("results"));
        log.info("%f", relevance.countExists() / (double)relevance.results.size());
    }
}
