package kba2sentences_ia;

import kba2sentence0.*;
import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import scrape.page.PageInputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.kba.InputFormatKBA;
import streamcorpus.sentence.SentenceWritable;
/**
 *
 * @author jeroen
 */
public class SentenceIAJob {
   public static final Log log = new Log( SentenceIAJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        job.setInputFormatClass(PageInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(SentenceIAMap.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setNumReduceTasks(1000);
        job.setReducerClass(SentenceIAReducer.class);
        job.setGroupingComparatorClass(IntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongWritable.SortComparator.class);
        job.setPartitionerClass(IntLongWritable.Partitioner.class);

        Path out = new Path(conf.get("output"));
        job.setOutputFormatClass(SentenceOutputFormat.class);
        SentenceOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).trash();
        
        job.waitForCompletion(true);
    }
   
}
