package kba2sentence0;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.IO.InputFormat;
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
        Configuration conf = new Configuration(args, "-i input -o output -j {jardir}");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        
        Job job = new Job(conf, "KBASentence0Job");
        
        String input = conf.get("input");        
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormat.setNonSplitable(job);
        SentenceInputFormat.addDirs(job, input); 
        
        job.setMapperClass(Sentence0Map.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.waitForCompletion(true);
    }
   
}
