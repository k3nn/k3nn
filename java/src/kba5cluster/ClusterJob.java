package kba5cluster;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ConfSetting;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;

public class ClusterJob {

    private static final Log log = new Log(ClusterJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String inputfilename = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input webpages paths: %s", inputfilename);
        log.info(" - output clickedurls path: %s", out);

        Job job = new Job(conf, inputfilename, out);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, inputfilename);
        SentenceInputFormat.setNonSplitable(job);

        //job.setNumReduceTasks(0);
        job.setMapperClass(ClusterMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
