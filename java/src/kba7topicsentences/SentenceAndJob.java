package kba7topicsentences;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.sentence.SentenceWritable;

/**
 * Create a list of all titles that contain all query terms
 * @author jeroen
 */
public class SentenceAndJob {

    private static final Log log = new Log(SentenceAndJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);

        String inputfilename = conf.get("input");
        Path out = new Path(conf.get("output"));
        setQueries(conf);

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", inputfilename);
        log.info(" - output: %s", out);
        log.info(" - topics: %s", conf.get("topicfile"));

        Job job = new Job(conf, inputfilename, out, conf.get("topicfile"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, inputfilename);

        job.setNumReduceTasks(10);
        job.setMapperClass(SentenceAndMap.class);
        job.setReducerClass(SentenceAndReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);

        job.setSortComparatorClass(IntLongWritable.DecreasingComparator.class);

        FileSystem.get(conf).delete(out, true);
        return job;
    }

    public static void setQueries(Conf conf) {
        TopicFile tf = new TopicFile(new Datafile(conf.get("topicfile")));
        for (TopicWritable topic : tf) {
            conf.addArray("topicquery", topic.query);
            conf.addArray("topicid", Integer.toString(topic.id));
            conf.addArray("topicstart", Long.toString(topic.start));
            conf.addArray("topicend", Long.toString(topic.end));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
