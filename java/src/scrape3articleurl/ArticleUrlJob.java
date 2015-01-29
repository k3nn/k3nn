package scrape3articleurl;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ConfSetting;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.TextLongWritable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import scrape.date.SnapshotOutputFormat;
import scrape.date.SnapshotWritable;

/**
 * Fetches all navigation pages at the given crawl times, to retrieve a list of
 * all hyperlinks tat match the regex of a news article as defined in DOMAIN.
 * @author jeroen
 */
public class ArticleUrlJob {
   public static final Log log = new Log( ArticleUrlJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setreduceMemoryMB(4096);
        conf.setTaskTimeout( 60000000 );
        conf.setReduceStart(0.95);

        String input = conf.get("input");
        String output = conf.get("output");
        Job job = new Job(conf, input, output);
              
        job.setInputFormatClass(NLineInputFormat.class); 
        NLineInputFormat.addInputPath(job, new Path(input));
        NLineInputFormat.setNumLinesPerSplit(job, 10);
        job.setMapperClass(ArticleUrlMap.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setMapOutputValueClass(SnapshotWritable.class);

        job.setGroupingComparatorClass(TextLongWritable.GroupComparator.class);
        job.setSortComparatorClass(TextLongWritable.SortComparator.class);
        job.setPartitionerClass(TextLongWritable.Partitioner.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(ArticleUrlReducer.class);

        Path out = new Path(output);
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setSingleOutput(job, out);
        //new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
