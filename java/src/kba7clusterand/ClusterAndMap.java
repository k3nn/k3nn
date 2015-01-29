package kba7clusterand;

import Cluster.ClusterWritable;
import KNN.Stream;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntIntWritable;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class ClusterAndMap extends Mapper<LongWritable, ClusterWritable, IntLongWritable, ClusterWritable> {

    public static final Log log = new Log(ClusterAndMap.class);
    Configuration conf;
    int currentcluster = -1;
    HashSet<Integer> matched = new HashSet();
    static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    public ArrayList<HashSet<String>> topicterms;
    public HashSet<String> allterms;
    public ArrayList<TopicWritable> topics;
    public ArrayList<ClusterWritable> list;
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        topics = this.getTopics(conf);
        topicterms = this.getTopicTerms(topics);
        allterms = this.allTopicTerms(topicterms);
    }

    @Override
    public void map(LongWritable key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        if (currentcluster > -1 && currentcluster != value.clusterid) {
            cleanup(context);
            currentcluster = -1;
        }
        if (value.clusterid >= 0) {
            if (currentcluster == -1) {
                currentcluster = value.clusterid;
                list = new ArrayList();
                if (matched.size() > 0) {
                    matched = new HashSet();
                }
            }
            list.add(value);
            ArrayList<String> tokenize = tokenizer.tokenize(value.title);
            for (String s : tokenize) {
                if (allterms.contains(s)) {
                    HashSet<String> titleterms = new HashSet(tokenize);
                    for (int i = 0; i < topics.size(); i++) {
                        if (titleterms.containsAll(topicterms.get(i))) {
                            matched.add(i);
                        }
                    }
                    break;
                }
            }
        } else {
            outUnclustered(value, context);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (matched.size() > 0)
        log.info("cleanup %s", matched);
        for (int topicid : matched) {
            for (ClusterWritable w : list) {
                log.info("%d %d", topicid, w.urlid);
                this.outkey.set(topicid, w.creationtime);
                context.write(outkey, w);
            }
        }
    }

    public void outUnclustered(ClusterWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<String> tokenize = tokenizer.tokenize(value.title);
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
