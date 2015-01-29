package stream1clusterbacklink;

import stream1cluster.*;
import StreamCluster.StreamClusterWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.IntPartitioner;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.sentence.SentenceWritable;

/**
 * This simulates a streaming process of the collection in parallel, by taking
 * the snapshot of yesterday midnight, and then processing the arrivals of a single
 * day as a stream. 
 * 
 * During the streaming, the clusters are monitored, whether they contain a sentence
 * that is targeted by a query. The output consists of cluster snapshot for every sentence
 * added to a cluster that (then) contains a targeted sentence. The snapshot contains all
 * sentences the cluster consists of at that point in time, the last sentence being the
 * one that can be used as output.
 * 
 * To make it easier, this routine uses an inventory of the sentences that
 * contain all query terms, for instance created by kba7topicallsentences.
 * @author jeroen
 */
public class StreamClusterJob {

    private static final Log log = new Log(StreamClusterJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -c clusters -o output -t topics");
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(1800000);
        conf.setMapSpeculativeExecution(false);
        conf.setReduceSpeculativeExecution(false);
        storeTopicSentences(conf);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));
        int reducers = topicFiles(conf).size();
        
        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - topics: %s", conf.get("topics"));
        log.info(" - clusters: %s", conf.get("clusters"));
        log.info(" - output: %s", out);
        log.info(" - reducers: %d", reducers);

        Job job = new Job(conf, input, conf.get("clusters"), conf.get("topics"), out);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        SentenceInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(reducers);
        job.setMapperClass(StreamClusterMap.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(StreamClusterWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setPartitionerClass(IntPartitioner.class);
        job.setReducerClass(StreamClusterReducer.class);
        
        job.waitForCompletion(true);
    }
    
    public static HashMap<Integer, ArrayList<Integer>> topicSentences(Configuration conf) throws IOException {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap();
        ArrayList<Datafile> files = topicFiles(conf);
        for (int reducer = 0; reducer < files.size(); reducer++) {
            Datafile df = files.get(reducer);
            SentenceFile sf = new SentenceFile(df);
            for (SentenceWritable w : sf) {
                ArrayList<Integer> list = result.get(w.id);
                if (list == null) {
                    list = new ArrayList();
                    result.put(w.id, list);
                }
                list.add(reducer);
            }
        }
        return result;
    }
    
    public static void storeTopicSentences(Configuration conf) throws IOException {
        HashMap<Integer, ArrayList<Integer>> topicSentences = topicSentences(conf);
        conf.setInt("topicsentences", topicSentences.size());
        int count = 0;
        for (Map.Entry<Integer, ArrayList<Integer>> entry : topicSentences.entrySet()) {
           conf.setInt(sprintf("topicsentence.key.%d", count), entry.getKey());
           Conf.setIntList(conf, sprintf("topicsentence.value.%d", count++), entry.getValue());
        }
    }
    
    public static HashMap<Integer, ArrayList<Integer>> getTopicSentences(Configuration conf) throws IOException {
        HashMap<Integer, ArrayList<Integer>> topicSentences = new HashMap();
        int size = conf.getInt("topicsentences", 0);
        for (int i = 0; i < size; i++) {
           int key = conf.getInt(sprintf("topicsentence.key.%d", i), 0);
           ArrayList<Integer> list = Conf.getIntList(conf, sprintf("topicsentence.value.%d", i));
           topicSentences.put(key, list);
        }
        return topicSentences;
    }
    
    public static ArrayList<Datafile> topicFiles(Configuration conf) throws IOException {
        HDFSPath path = new HDFSPath(conf, conf.get("topics"));
        return path.getFiles("*.\\d+");
    }
        

}
