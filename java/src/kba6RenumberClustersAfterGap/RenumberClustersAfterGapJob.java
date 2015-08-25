package kba6RenumberClustersAfterGap;

import ClusterNode.ClusterNodeInputFormat;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;

/**
 * The KBA corpus contains a gap of a few weeks, that may cause the cluster
 * numbering to reset to 0. This job fixes the cluster numbering by incrementing
 * a constant to all clusters after that gap.
 * @author jeroen
 */
public class RenumberClustersAfterGapJob {

    private static final Log log = new Log(RenumberClustersAfterGapJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output -inc increment");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input webpages paths: %s", input);
        log.info(" - output clickedurls path: %s", out);

        Job job = new Job(conf, out, conf.get("increment"));

        job.setInputFormatClass(ClusterNodeInputFormat.class);
        ClusterNodeInputFormat.addDirs(job, input);
        ClusterNodeInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(0);
        job.setMapperClass(RenumberClustersAfterGapMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
