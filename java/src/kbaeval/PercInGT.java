package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.hadoop.Conf;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import io.github.htools.type.Tuple2;
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
       ClusterFile tf = new ClusterFile(df);
       for (ClusterWritable t : tf) {
           for (NodeWritable u : t.nodes) {
              String updateid = sprintf("%s-%d", u.docid, u.sentenceNumber);
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
