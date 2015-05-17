package stream5Retrieve.Unigram;

import stream5Retrieve.NoTitle.*;
import static stream5Retrieve.RetrieveJob.*;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.io.HDFSPath;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class RetrieveJob {

    private static final Log log = new Log(RetrieveJob.class);
    static Conf conf; 

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);
        conf.setMapSpeculativeExecution(false);
        conf.setMapMemoryMB(8192);
        conf.setReduceMemoryMB(4096);
        conf.setTaskTimeout(3600000);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - topicfile: %s", conf.get("topicfile"));

        Job job = new Job(conf, input, out, conf.get("topicfile"));
        //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

        job.setInputFormatClass(InputFormat.class);
        addInput(job, new HDFSPath(conf, conf.get("input")), conf.get("topicfile"));

        job.setNumReduceTasks(getParams().size());
        job.setMapperClass(RetrieveMap.class);
        job.setReducerClass(RetrieveReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Setting.class);
        job.setMapOutputValueClass(MatchingClusterNodeWritable.class);
        job.setPartitionerClass(SettingPartitioner.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
