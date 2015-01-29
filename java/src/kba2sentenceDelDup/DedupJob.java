package kba2sentenceDelDup;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
/**
 *
 * @author jeroen
 */
public class DedupJob {
   public static final Log log = new Log( DedupJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setMapSpeculativeExecution(false);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(DedupMap.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
   
}
