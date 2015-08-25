package kba7TopicMatchingSentences;

import KNN.Stream;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import Sentence.SentenceWritable;

/**
 * Route titles that contain all terms to one topic, to the reducer of that topic.
 * @author jeroen
 */
public class TopicMatchingSentencesMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(TopicMatchingSentencesMap.class);
    Configuration conf;
    public static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    public ArrayList<HashSet<String>> topicterms;
    public HashSet<String> allterms;
    public ArrayList<TopicWritable> topics;
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        topics = TopicMatchingSentencesJob.getTopics(conf);
        topicterms = TopicMatchingSentencesJob.getTopicTerms(topics);
        allterms = TopicMatchingSentencesJob.allTopicTerms(topicterms);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<String> tokenize = tokenizer.tokenize(value.content);
        for (String s : tokenize) {
            if (allterms.contains(s)) {
                HashSet<String> titleterms = new HashSet(tokenize);
                for (int i = 0; i < topics.size(); i++) {
                    if (titleterms.containsAll(topicterms.get(i))) {
                        TopicWritable topic = topics.get(i);
                        if (value.creationtime >= topic.start && value.creationtime <= topic.end) {
                           outkey.set(i, value.creationtime);
                           context.write(outkey, value);
                        }
                    }
                }
                break;
            }
        }
    }
}
