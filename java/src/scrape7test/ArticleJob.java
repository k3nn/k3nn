package scrape7test;

import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import scrape.page.PageInputFormat;
import scrape.page.PageOutputFormat;
import scrape.page.PageWritable;
/**
 *
 * @author jeroen
 */
public class ArticleJob {
   public static final Log log = new Log( ArticleJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 4096);
        conf.setInt("mapreduce.reduce.memory.mb", 4096);
        conf.set("mapreduce.map.java.opts", "-server -Xmx3584m");
        conf.set("mapreduce.reduce.java.opts", "-server -Xmx3584m");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.95);
        conf.setInt("mapreduce.task.timeout", 60000000);
        String input = conf.get("input");
        Job job = new Job(conf, input, conf.get("output"));
              
        job.setInputFormatClass(PageInputFormat.class); 
        PageInputFormat.addInputPath(job, new Path(input));
        PageInputFormat.setNonSplitable(job);
        job.setMapperClass(ArticleMap.class);
        job.setNumReduceTasks(0);
        
        job.setOutputFormatClass(NullOutputFormat.class);
        //PageOutputFormat.setOutputPath(job, new Path(conf.get("output"))); // for logfile

        Path out = new Path(conf.get("output"));
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
