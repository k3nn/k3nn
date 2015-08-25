package kba3SentencesToTitlesOriginal;

import kba3SentencesToTitles.*;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.InputFormat;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;

/**
 * Creates a file of only the titles of News Articles
 * @author jeroen
 */
public class SentenceToTitlesJob {
   public static final Log log = new Log( SentenceToTitlesJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setMapSpeculativeExecution(false);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(SentenceToTitlesMap.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
   
}
