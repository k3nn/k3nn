package kba1raw;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.io.IntLongWritable;
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
public class KBAJob {
   public static final Log log = new Log( KBAJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output -f firstday");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);
        
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        String input = conf.get("input");
        HDFSPath in = new HDFSPath(conf, input);
        job.setInputFormatClass(InputFormatKBAGZ.class);
        InputFormatKBA.addDirs(job, in);
        //ReducerKeysDays reducerkeys = new ReducerKeysDays(job.getConfiguration());
        
        job.setMapperClass(KBAMap.class);
        job.setMapOutputKeyClass(IntLongStringIntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntLongStringIntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongStringIntWritable.SortComparator.class);
        //job.setPartitionerClass(IntLongStringIntWritable.Partitioner.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(KBAReducer.class);

        Path out = new Path(conf.get("output"));
        new SentenceOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        //new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(false);
    }
   
}
