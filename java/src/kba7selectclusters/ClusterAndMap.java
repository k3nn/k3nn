package kba7selectclusters;

import kba7clusterand.*;
import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Url;
import KNN.UrlT;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntIntWritable;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class ClusterAndMap extends Mapper<LongWritable, ClusterWritable, IntLongWritable, StreamClusterWritable> {

    public static final Log log = new Log(ClusterAndMap.class);
    Configuration conf;
    int currentcluster = -1;
    HashMap<Integer, Long> matched = new HashMap();
    static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    public ArrayList<HashSet<String>> topicterms;
    public HashSet<String> allterms;
    public ArrayList<TopicWritable> topics;
    public ArrayMap3<Integer, String, String> nn;
    IntLongWritable outkey = new IntLongWritable();
    Stream<UrlT> stream;
    Cluster<UrlT> cluster;
    Sorter sorter = new Sorter();

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
                stream = new Stream();
                nn = new ArrayMap3();
                currentcluster = value.clusterid;
                cluster = stream.createCluster(currentcluster);
                matched = new HashMap();
            }
            ArrayList<String> tokenize = tokenizer.tokenize(value.title);
            UrlT url = new UrlT(value.urlid, value.domain, value.title, tokenize, value.creationtime);
            url.setCluster(cluster);
            for (String s : tokenize) {
                if (allterms.contains(s)) {
                    HashSet<String> titleterms = new HashSet(tokenize);
                    for (int i = 0; i < topics.size(); i++) {
                        if (titleterms.containsAll(topicterms.get(i))) {
                            Long previoustime = matched.get(i);
                            if (previoustime == null || previoustime > url.getCreationTime()) {
                                matched.put(i, url.getCreationTime());
                            }
                        }
                    }
                    break;
                }
            }
            stream.urls.put(url.getID(), url);
            nn.add(url.getID(), value.nnid, value.nnscore);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (matched.size() > 0) {
            log.info("cleanup %s", matched);
            for (Map.Entry<Integer, Tuple2<String, String>> entry : nn) {
                Url u = stream.urls.get(entry.getKey());
                ArrayList<Integer> nnid = this.getNN(entry.getValue().value1);
                ArrayList<Double> nnscore = this.getScores(entry.getValue().value2);
                for (int i = 0; i < nnid.size(); i++) {
                    Edge e = new Edge(stream.urls.get(nnid.get(i)), nnscore.get(i));
                    u.add(e);
                }
            }
            cluster.setBase(Cluster.getBase(cluster.getUrls()));
            long clustercreationtime = 0;
            for (Url u : cluster.getBase()) {
                if (u.getCreationTime() > clustercreationtime) {
                    clustercreationtime = u.getCreationTime();
                }
            }
            for (Map.Entry<Integer, Long> topic : matched.entrySet()) {
                long creationtime = Math.max(topic.getValue(), clustercreationtime);
                ArrayList<UrlWritable> list = new ArrayList();
                for (Url u : cluster.getUrls()) {
                    UrlWritable uw = new UrlWritable();
                    uw.creationtime = u.getCreationTime();
                    uw.domain = u.getDomain();
                    uw.nnid = u.getNN();
                    uw.nnscore = u.getScore();
                    uw.title = u.getTitle();
                    uw.urlid = u.getID();
                    list.add(uw);
                }
                Collections.sort(list, sorter);
                StreamClusterWritable w = new StreamClusterWritable();
                w.clusterid = cluster.getID();
                for (UrlWritable u : list) {
                    w.urls.add(u);
                    if (u.creationtime >= creationtime) {
                        this.outkey.set(topic.getKey(), u.creationtime);
                        context.write(outkey, w);
                    }
                }
            }
        }
    }

    private class Sorter implements Comparator<UrlWritable> {

        @Override
        public int compare(UrlWritable o1, UrlWritable o2) {
            return (int) (o1.creationtime - o2.creationtime);
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

    public ArrayList<Integer> getNN(String nn) {
        ArrayList<Integer> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Integer.parseInt(n));
        }
        return result;
    }

    public ArrayList<Double> getScores(String nn) {
        ArrayList<Double> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Double.parseDouble(n));
        }
        return result;
    }

}
