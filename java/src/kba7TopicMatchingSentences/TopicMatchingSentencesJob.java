package kba7TopicMatchingSentences;

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
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;
import java.util.ArrayList;
import java.util.HashSet;
import static kba7TopicMatchingSentences.TopicMatchingSentencesMap.tokenizer;
import org.apache.hadoop.conf.Configuration;

/**
 * Create a list of all titles that contain all query terms. This list is used 
 * to flag "query matching clusters".
 * @author jeroen
 */
public class TopicMatchingSentencesJob {

    private static final Log log = new Log(TopicMatchingSentencesJob.class);
    private static long T_1 = 60 * 60 * 24 * 4;

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);

        String inputfilename = conf.get("input");
        Path out = new Path(conf.get("output"));
        setTopics(conf);

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", inputfilename);
        log.info(" - output: %s", out);
        log.info(" - topics: %s", conf.get("topicfile"));

        Job job = new Job(conf, inputfilename, out, conf.get("topicfile"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, inputfilename);

        job.setNumReduceTasks(10);
        job.setMapperClass(TopicMatchingSentencesMap.class);
        job.setReducerClass(TopicMatchingSentencesReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);

        job.setSortComparatorClass(IntLongWritable.DecreasingComparator.class);

        FileSystem.get(conf).delete(out, true);
        return job;
    }

    /**
     * Read topics from local FS, and store these in the Configuration
     * @param conf 
     */
    public static void setTopics(Conf conf) {
        TopicFile tf = new TopicFile(new Datafile(conf.get("topicfile")));
        for (TopicWritable topic : tf) {
            conf.addArray("topicquery", topic.query);
            conf.addArray("topicid", Integer.toString(topic.id));
            conf.addArray("topicstart", Long.toString(topic.start));
            conf.addArray("topicend", Long.toString(topic.end));
        }
    }

    /**
     * @param conf
     * @return list of topics read from Configuration
     */
    public static ArrayList<TopicWritable> getTopics(Configuration conf) {
        ArrayList<TopicWritable> result = new ArrayList();
        String[] topics = conf.getStrings("topicquery");
        String[] ids = conf.getStrings("topicid");
        String[] starts = conf.getStrings("topicstart");
        String[] ends = conf.getStrings("topicend");
        for (int i = 0; i < topics.length; i++) {
            TopicWritable t = new TopicWritable();
            t.query = topics[i];
            t.id = Integer.parseInt(ids[i]);
            t.start = Long.parseLong(starts[i])  - T_1;
            t.end = Long.parseLong(ends[i]);
            result.add(t);
        }
        return result;
    }

    /**
     * @param topics
     * @return list of topics, with for each topic a list of unique query terms
     */
    public static ArrayList<HashSet<String>> getTopicTerms(ArrayList<TopicWritable> topics) {
        ArrayList<HashSet<String>> result = new ArrayList();
        for (TopicWritable t : topics) {
            ArrayList<String> tokenize = tokenizer.tokenize(t.query);
            result.add(new HashSet(tokenize));
        }
        return result;
    }

    /**
     * @param topics
     * @return set containing query terms of all topics combined
     */
    public static HashSet<String> allTopicTerms(ArrayList<HashSet<String>> topicterms) {
        HashSet<String> allterms = new HashSet();
        for (HashSet<String> set : topicterms) {
            allterms.addAll(set);
        }
        return allterms;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
