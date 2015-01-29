package kbaeval;

import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
/**
 *
 * @author jeroen
 */
public class SwitchDuplicates {
   public static final Log log = new Log( SwitchDuplicates.class );
   HashMap<String, String> matches;

   public SwitchDuplicates(String duplicatesfile, String resultsfile, String outfile) {
       HashMap<String, TrecWritable> readResults = readResults(resultsfile, outfile);
       readMatches(readResults, duplicatesfile);
       writeResults(outfile, readResults.values());
   }
   
   public void readMatches(HashMap<String, TrecWritable> results, String duplicatesfile) {
       Datafile df = new Datafile(duplicatesfile);
       df.setBufferSize(100000000);
       DuplicateFile mf = new DuplicateFile(df);
       for (DuplicateWritable d : mf) {
           TrecWritable get = results.get(d.duplicate);
           if (get != null && !d.original.equals("NULL")) {
              int index = d.original.lastIndexOf("-");
              get.document = d.original.substring(0, index);
              get.sentence = Integer.parseInt(d.original.substring(index+1));
           }
       }
   }
   
   public HashMap<String, TrecWritable> readResults(String resultsfile, String outfile) {
       HashMap<String, TrecWritable> results = new HashMap();
       Datafile df = new Datafile(resultsfile);
       TrecFile tf = new TrecFile(df);
       for (TrecWritable t : tf) {
           String id = t.document + "-" + t.sentence;
           results.put(id, t);
       }
       return results;
   }
   
   public void writeResults(String outfile, Collection<TrecWritable> results) {
       Datafile outdf = new Datafile(outfile);
       TrecFile outtf = new TrecFile(outdf);
       outtf.openWrite();
       for (TrecWritable t : results) {
           t.write(outtf);
       }
       outtf.closeWrite();
   }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "-m matches -r results -o outfile");
        SwitchDuplicates relevance = new SwitchDuplicates(ap.get("matches"), 
                ap.get("results"), ap.get("outfile"));
    }
}
