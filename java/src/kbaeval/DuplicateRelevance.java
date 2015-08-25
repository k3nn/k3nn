package kbaeval;

import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class DuplicateRelevance {
   public static final Log log = new Log( DuplicateRelevance.class );
   HashMap<String, ArrayList<MatchesWritable>> matches;
   ArrayMap<MatchingClusterNodeWritable, String> results;

   public DuplicateRelevance(String matchesfile, String poolfile, String matchduplicatefile) {
       matches = readMatches(matchesfile);
       readPool(poolfile, matchduplicatefile);
   }
   
   public HashMap<String, ArrayList<MatchesWritable>> readMatches(String matchesfile) {
       Datafile df = new Datafile(matchesfile);
       MatchesFile mf = new MatchesFile(df);
       return mf.getMap();
   }
   
   public void readPool(String poolfile, String matchduplicatefile) {
       Datafile dfdup = new Datafile(matchduplicatefile);
       MatchesFile newmatches = new MatchesFile(dfdup);
       newmatches.openWrite();
       for (ArrayList<MatchesWritable> list : matches.values()) {
           for (MatchesWritable m : list) {
               m.write(newmatches);
           }
       }
       Datafile df = new Datafile(poolfile);
       DuplicateFile tf = new DuplicateFile(df);
       for (DuplicateWritable t : tf) {
           ArrayList<MatchesWritable> get = matches.get(t.original);
           if (get != null) {
               for (MatchesWritable m : get) {
                   m.updateid = t.duplicate;
                   m.write(newmatches);
               }
           }
       }
       newmatches.closeWrite();
   }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "-m matches -r results -d duplicatematch");
        DuplicateRelevance relevance = new DuplicateRelevance(ap.get("matches"), ap.get("results"), ap.get("duplicatematch"));
    }
}
