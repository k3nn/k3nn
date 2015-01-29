package kba7topicsentences;

import KNN.Stream;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class SentenceAndMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(SentenceAndMap.class);
    Configuration conf;
    static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    public ArrayList<HashSet<String>> topicterms;
    public HashSet<String> allterms;
    public ArrayList<TopicWritable> topics;
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        topics = this.getTopics(conf);
        topicterms = this.getTopicTerms(topics);
        allterms = this.allTopicTerms(topicterms);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
            ArrayList<String> tokenize = tokenizer.tokenize(value.sentence);
            for (String s : tokenize) {
                if (allterms.contains(s)) {
                    HashSet<String> titleterms = new HashSet(tokenize);
                    for (int i = 0; i < topics.size(); i++) {
                        if (titleterms.containsAll(topicterms.get(i))) {
                           outkey.set(i, value.creationtime);
                           context.write(outkey, value);
                        }
                    }
                    break;
                }
            }
    }

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
            t.start = Long.parseLong(starts[i]);
            t.end = Long.parseLong(ends[i]);
            result.add(t);
        }
        return result;
    }

    public static ArrayList<HashSet<String>> getTopicTerms(ArrayList<TopicWritable> topics) {
        ArrayList<HashSet<String>> result = new ArrayList();
        for (TopicWritable t : topics) {
            ArrayList<String> tokenize = tokenizer.tokenize(t.query);
            result.add(new HashSet(tokenize));
        }
        return result;
    }

    public static HashSet<String> allTopicTerms(ArrayList<HashSet<String>> topicterms) {
        HashSet<String> allterms = new HashSet();
        for (HashSet<String> set : topicterms) {
            allterms.addAll(set);
        }
        return allterms;
    }

}
