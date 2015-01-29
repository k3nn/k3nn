package kba6finalcluster;

import Cluster.ClusterWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ConfSetting;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntIntWritable;
import io.github.repir.tools.hadoop.io.NullInputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * This setup was for retrospective temporal summarization, to determine the final
 * state of each cluster, which is then used for summarization.
 * @author jeroen
 */
public class FinalJob {

    private static final Log log = new Log(FinalJob.class);

    public static Job setup(String args[]) throws IOException, ParseException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setReduceStart(0.95);
        conf.setReduceSpeculativeExecution(false);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);

        Job job = new Job(conf, input, out);

        job.setInputFormatClass(NullInputFormat.class);

        job.setNumReduceTasks(FinalMap.getDates(conf).size());
        log.info("dates %s", FinalMap.getDates(conf));
        
        job.setMapperClass(FinalMap.class);
        job.setReducerClass(FinalReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.setMapOutputKeyClass(IntIntWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);

        job.setPartitionerClass(IntIntWritable.Partitioner.class);
        job.setGroupingComparatorClass(IntIntWritable.Comparator.class);
        job.setSortComparatorClass(IntIntWritable.SortComparator.class);

        FileSystem.get(conf).delete(out, true);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
