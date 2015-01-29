package scrape6article;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import scrape.page.PageInputFormat;
import scrape.page.PageWritable;

/**
 * Sort the crawled articles on estimated creation time, and store them in files
 * per day.
 * @author jeroen
 */
public class DaySortJob {
   public static final Log log = new Log( DaySortJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(8192);
        conf.setreduceMemoryMB(4096);
        conf.setReduceStart(0.95);
        conf.setTaskTimeout(60000000);
        conf.setMapSpeculativeExecution(false);
        conf.setReduceSpeculativeExecution(false);
        conf.setInt("mapreduce.task.io.sort.mb", 800);
        
        DayPartitioner.setTime(conf, "2011-09-01", "2013-02-13");
        String input = conf.get("input");
        Job job = new Job(conf, input, conf.get("output"));
              
        job.setInputFormatClass(PageInputFormat.class); 
        PageInputFormat.addDirs(job, input);
        PageInputFormat.setNonSplitable(job);
        job.setMapperClass(DaySortMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PageWritable.class);

        job.setSortComparatorClass(LongWritable.Comparator.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setNumReduceTasks(DayPartitioner.getNumberOfReducers(conf));
        
        job.setReducerClass(DaySortReducer.class);
        
        job.setOutputFormatClass(NullOutputFormat.class);
        OutputFormat.setOutputPath(job, new Path(conf.get("output"))); // for logfile

        //Path out = new Path(conf.get("output"));
        //new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
