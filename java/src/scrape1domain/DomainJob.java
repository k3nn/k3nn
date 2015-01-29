package scrape1domain;

import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.NullInputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import scrape.date.SnapshotOutputFormat;

/**
 * For all the sites in DOMAIN, grab the root page from the Internet Archive to
 * store all times that site was crawled, within given date range.
 * @author jeroen
 */
public class DomainJob {
   public static final Log log = new Log( DomainJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-o output -s startdate -e enddate");
        conf.setMapMemoryMB(4096);
        
        Job job = new Job(conf, conf.get("output"), conf.get("startdate"), conf.get("enddate"));
        if (!DateTools.FORMAT.Y_M_D.isValid(conf.get("startdate"))) {
            log.fatal("invalid startdate %s", conf.get("startdate"));
        }
        
        if (!DateTools.FORMAT.Y_M_D.isValid(conf.get("enddate"))) {
            log.fatal("invalid enddate %s", conf.get("enddate"));
        }
        
        job.setInputFormatClass(NullInputFormat.class); 
        
        job.setMapperClass(DomainMap.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(Domain_IA.instance.getDomains().length);
        job.setReducerClass(DomainReducer.class);

        Path out = new Path(conf.get("output"));
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
