package scrape4merge;

import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.TextLongWritable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import scrape.date.SnapshotInputFormat;
import scrape.date.SnapshotOutputFormat;
import scrape.date.SnapshotWritable;

/**
 * The article URLs are grouped by URL and sorted by creation time ascending,
 * to keep only the first version of each URL.
 * @author jeroen
 */
public class MergeJob {
   public static final Log log = new Log( MergeJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output -r reducers");
        conf.setMapMemoryMB(4096);
        conf.setreduceMemoryMB(4096);
        conf.setTaskTimeout( 60000000 );
        conf.setReduceStart(0.95);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        String output = conf.get("output");
        Job job = new Job(conf, input, output);
              
        job.setInputFormatClass(SnapshotInputFormat.class); 
        SnapshotInputFormat.addInputPath(job, new Path(input));
        job.setMapperClass(MergeMap.class);
        job.setMapOutputKeyClass(TextLongWritable.class);
        job.setMapOutputValueClass(SnapshotWritable.class);

        job.setGroupingComparatorClass(TextLongWritable.GroupComparator.class);
        job.setSortComparatorClass(TextLongWritable.SortComparator.class);
        job.setPartitionerClass(TextLongWritable.Partitioner.class);
        job.setNumReduceTasks(conf.getInt("reducers", 100));
        job.setReducerClass(MergeReducer.class);

        Path out = new Path(output);
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
