package kba3dedup;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.kba.InputFormatKBA;
import streamcorpus.sentence.SentenceWritable;
/**
 *
 * @author jeroen
 */
public class DedupJob {
   public static final Log log = new Log( DedupJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        job.setNumReduceTasks(1000);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        
        job.setMapperClass(DedupMap.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongWritable.SortComparator.class);
        job.setPartitionerClass(IntLongWritable.Partitioner.class);
        job.setReducerClass(DedupReducer.class);
        
        Path out = new Path(conf.get("output"));
        job.setOutputFormatClass(SentenceOutputFormat.class);
        SentenceOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(false);
    }
}
