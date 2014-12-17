package kba3dedup;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.IO.IntLongWritable;
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
        Configuration conf = new Configuration(args, "-i input -o output -j {jardir}");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        
        String input = conf.get("input");        
        Job job = new Job(conf, "KBADedupJob [%s] [%s]", 
                input, conf.get("output"));
        
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
        new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
}
