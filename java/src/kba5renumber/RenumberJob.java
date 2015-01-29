package kba5renumber;

import Cluster.ClusterInputFormat;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;

public class RenumberJob {

    private static final Log log = new Log(RenumberJob.class);

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

        job.setInputFormatClass(ClusterInputFormat.class);
        ClusterInputFormat.addDirs(job, input);
        ClusterInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(0);
        job.setMapperClass(RenumberMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
