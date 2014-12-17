package kba4sort;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.DateTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.IO.DayPartitioner;
import io.github.repir.tools.hadoop.IO.IntLongWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.kba.InputFormatKBA;
import streamcorpus.sentence.SentenceWritable;
/**
 *
 * @author jeroen
 */
public class SortJob {
   public static final Log log = new Log( SortJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
        Configuration conf = new Configuration(args, "-i input -o output -s startdate -e enddate -j {jardir}");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        conf.setBoolean("mapreduce.reduce.speculative", false);

        DayPartitioner.setTime(conf, conf.get("startdate"), conf.get("enddate"));
        
        String input = conf.get("input");        
        Job job = new Job(conf, "KBADayJob [%s] [%s] [%s-%s]", 
                input, conf.get("output"), conf.get("startdate"), conf.get("enddate"));
        
        int reducers = DayPartitioner.getReducerNumber(conf);
        log.info("reducers %d", reducers);
        job.setNumReduceTasks(reducers);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        
        job.setMapperClass(SortMap.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setSortComparatorClass(LongWritable.Comparator.class);
        job.setPartitionerClass(DayPartitioner.class);
        job.setReducerClass(SortReducer.class);
        
        Path out = new Path(conf.get("output"));
        job.setOutputFormatClass(NullOutputFormat.class);
        new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
