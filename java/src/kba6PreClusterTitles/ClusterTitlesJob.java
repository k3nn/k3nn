package kba6PreClusterTitles;

import io.github.htools.lib.Log;
import io.github.htools.hadoop.ConfSetting;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;

/**
 * Cluster the titles, simulating an online setting, streaming the titles
 * one-at-a-time. This requires the titles to be processed in sequence, which 
 * cannot be done in parallel. For easy inspection of the results, and to allow
 * consecutive steps to be processed in parallel, we cluster the titles one day
 * at a time, based on the end state at midnight of the previous day, and storing
 * a snapshot of the title clustering at midnight at the end of the day. 
 * 
 * @author jeroen
 */
public class ClusterTitlesJob {

    private static final Log log = new Log(ClusterTitlesJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);

        String inputfilename = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", inputfilename);
        log.info(" - output: %s", out);

        Job job = new Job(conf, inputfilename, out);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, inputfilename);
        SentenceInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(0);
        job.setMapperClass(ClusterTitlesMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.waitForCompletion(true);
    }
}
