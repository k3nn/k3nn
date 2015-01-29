package kba2sentence0;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.kba.InputFormatKBA;
/**
 *
 * @author jeroen
 */
public class Sentence0Job {
   public static final Log log = new Log( Sentence0Job.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setMapSpeculativeExecution(false);
        
        String input = conf.get("input");        
        Job job = new Job(conf, input, conf.get("output"));
        
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(Sentence0Map.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
   
}
