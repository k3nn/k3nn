package scrape1.job;

import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.DateTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.IO.NullInputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import scrape.date.SnapshotOutputFormat;
/**
 *
 * @author jeroen
 */
public class SnapshotJob {
   public static final Log log = new Log( SnapshotJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(args, "-o output -s startdate -e enddate -j {jardir}");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        
        Job job = new Job(conf, "SnapshotJob");
        if (!DateTools.FORMAT.Y_M_D.isValid(conf.get("startdate"))) {
            log.fatal("invalid startdate %s", conf.get("startdate"));
        }
        
        if (!DateTools.FORMAT.Y_M_D.isValid(conf.get("enddate"))) {
            log.fatal("invalid enddate %s", conf.get("enddate"));
        }
        
        job.setInputFormatClass(NullInputFormat.class); 
        
        job.setMapperClass(SnapshotMap.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(Domain_IA.instance.getDomains().length);
        job.setReducerClass(SnapshotReducer.class);

        Path out = new Path(conf.get("output"));
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
