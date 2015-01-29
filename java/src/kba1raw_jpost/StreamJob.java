package kba1raw_jpost;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
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
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(8192);
        
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
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
