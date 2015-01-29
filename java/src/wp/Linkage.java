package wp;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import org.apache.hadoop.conf.Configuration;
import scrape.page.PageFile;
/**
 *
 * @author jeroen
 */
public class Linkage {
   public static final Log log = new Log( Linkage.class );
   LinkFile lf;
   
   public Linkage(Datafile df) {
       
   }
   
   public void read(Datafile df) {
       lf = new LinkFile(df);
       lf.setBufferSize(1000000);
       for ( LinkWritable l : lf ) {
        
       }
   }
   
    public static void main(String[] args) {
        ArgsParser ap = new ArgsParser(args, "input");
        Datafile df = new Datafile(new Configuration(), ap.get("input"));
    }
   
}
