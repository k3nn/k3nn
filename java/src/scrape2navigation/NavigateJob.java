package scrape2navigation;

import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ConfSetting;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.RandomWritable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import scrape.date.SnapshotOutputFormat;
import scrape.date.SnapshotWritable;

/**
 * For all the sites in DOMAIN and the crawl times, visit all pages within one click 
 * of the main page that are not articles (as defined by the regex in DOMAIN).
 * This is considered to contain all navigation pages that list the articles on the site.
 * @author jeroen
 */
public class NavigateJob {
   public static final Log log = new Log( NavigateJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output -r reducers");
        conf.setMapMemoryMB(4096);
        conf.setreduceMemoryMB(4096);
        conf.setTaskTimeout( 60000000 );
        conf.setReduceStart(0.95);
        
        String input = conf.get("input");
        int reducers = conf.getInt("reducers", 1000);
        Job job = new Job(conf, input, conf.get("output"), reducers);
              
        job.setInputFormatClass(NLineInputFormat.class); 
        NLineInputFormat.addInputPath(job, new Path(input));
        NLineInputFormat.setNumLinesPerSplit(job, 10);
        job.setMapperClass(NavigateMap.class);
        job.setMapOutputKeyClass(RandomWritable.class);
        job.setMapOutputValueClass(SnapshotWritable.class);

        job.setNumReduceTasks(reducers);
        job.setReducerClass(NavigateReducer.class);

        Path out = new Path(conf.get("output"));
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
