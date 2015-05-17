package kba2RemoveDuplicates;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import io.github.repir.tools.hadoop.io.LongLongWritable;
import java.text.ParseException;

/**
 * Removes duplicates that have the exact same documentID in the collection,
 * within a document duplicate sentences that have the same sentence number, 
 * and strips the extracted title from non-title elements.
 * @author jeroen
 */
public class RemoveDuplicatesJob {
   public static final Log log = new Log( RemoveDuplicatesJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Conf conf = new Conf(args, "-i input -o output -s startdate -e enddate");
        conf.setMapMemoryMB(4096);
        conf.setMapSpeculativeExecution(false);
        DayPartitioner.setTime(conf, conf.get("startdate"), conf.get("enddate"));
        int reducers = DayPartitioner.getNumberOfReducers(conf);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        log.info("reducers %d", reducers);
        job.setNumReduceTasks(reducers);
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(RemoveDuplicatesMap.class);
        job.setReducerClass(RemoveDuplicatesReducer.class);
        job.setMapOutputKeyClass(LongLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setSortComparatorClass(LongLongWritable.SortComparator.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
   
}
