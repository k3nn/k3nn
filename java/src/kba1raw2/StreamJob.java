package kba1raw2;

import kba1raw.*;
import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.IO.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.IO.IntLongWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import streamcorpus.sentence.SentenceWritable;
import streamcorpus.kba.InputFormatKBA;
import streamcorpus.kba.InputFormatKBAGZ;
/**
 *
 * @author jeroen
 */
public class StreamJob {
   public static final Log log = new Log( StreamJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(args, "-i input -o output -j {jardir}");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        
        Job job = new Job(conf, "KBAJob [%s] [%s]", conf.get("input"), conf.get("output"));
        
        String input = conf.get("input");
        HDFSPath in = new HDFSPath(conf, input);
        job.setInputFormatClass(InputFormatKBAGZ.class);
        InputFormatKBA.addDirs(job, in);
        //ReducerKeysDays reducerkeys = new ReducerKeysDays(job.getConfiguration());
        
        job.setMapperClass(StreamMap.class);
        job.setMapOutputKeyClass(IntLongStringIntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntLongStringIntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongStringIntWritable.SortComparator.class);
        //job.setPartitionerClass(IntLongStringIntWritable.Partitioner.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(StreamReducer.class);

        Path out = new Path(conf.get("output"));
        new SentenceOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        //new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(false);
    }
   
}
