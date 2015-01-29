package scrape5article;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import scrape.page.PageOutputFormat;
import scrape.page.PageWritable;

/**
 * Crawl the first version of the news articles for the DOMAINs. 
 * @author jeroen
 */
public class ArticleJob {
   public static final Log log = new Log( ArticleJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setreduceMemoryMB(4096);
        conf.setReduceStart(0.95);
        conf.setTaskTimeout(60000000);

        DayPartitioner.setTime(conf, "2011-09-01", "2013-02-13");
        String input = conf.get("input");
        Job job = new Job(conf, input, conf.get("output"));
              
        job.setInputFormatClass(NLineInputFormat.class); 
        NLineInputFormat.addInputPath(job, new Path(input));
        NLineInputFormat.setNumLinesPerSplit(job, 10);
        job.setMapperClass(ArticleMap.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PageWritable.class);
        job.setReducerClass(ArticleReducer.class);

        job.setOutputFormatClass(PageOutputFormat.class);
        OutputFormat.setSingleOutput(job, new Path(conf.get("output"))); // for logfile
        
        job.waitForCompletion(true);
    }
   
}
