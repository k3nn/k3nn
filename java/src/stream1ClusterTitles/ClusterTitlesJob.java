package stream1ClusterTitles;

import Cluster.ClusterWritable;
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
import Sentence.SentenceFile;
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;

/**
 * This simulates a streaming process of the collection in parallel, by taking
 * the snapshot of yesterday midnight, and then processing the arrivals of a single
 * day as a stream. 
 * 
 * During the streaming, the clusters are monitored, whether they contain a sentence
 * that is targeted by a query. The output consists of cluster snapshot for every sentence
 * added to a cluster that (then) contains a targeted sentence. The snapshot contains all
 * sentences the cluster consists of at that point in time, the last sentence being the
 * candidate Node that can qualify for emission to the user.
 * 
 * To make it easier, this routine uses an inventory of the sentences that
 * contain all query terms, for instance created by kba7TopicMatchingSentences.
 * @author jeroen
 */
public class ClusterTitlesJob {

    private static final Log log = new Log(ClusterTitlesJob.class);

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
        job.setMapperClass(ClusterTitlesMap.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setPartitionerClass(IntPartitioner.class);
        job.setReducerClass(ClusterTitlesReducer.class);
        
        job.waitForCompletion(true);
    }
    
    /**
     * 
     * @param conf
     * @return map of topics, with a list of sentences that contain all query
     * terms for that topic. Uses lists extracted by f.i. kba7TopicMatchingSentences
     * @throws IOException 
     */
    public static HashMap<Long, ArrayList<Integer>> topicSentences(Configuration conf) throws IOException {
        HashMap<Long, ArrayList<Integer>> result = new HashMap();
        ArrayList<Datafile> files = topicFiles(conf);
        for (int reducer = 0; reducer < files.size(); reducer++) {
            Datafile df = files.get(reducer);
            SentenceFile sf = new SentenceFile(df);
            for (SentenceWritable w : sf) {
                ArrayList<Integer> list = result.get(w.sentenceID);
                if (list == null) {
                    list = new ArrayList();
                    result.put(w.sentenceID, list);
                }
                list.add(reducer);
            }
        }
        return result;
    }
    
    /**
     * store the topics in the test set, with a list of sentence IDs of titles that contain all
     * query terms, in the configuration
     * @param conf
     */
    public static void storeTopicSentences(Configuration conf) throws IOException {
        HashMap<Long, ArrayList<Integer>> topicSentences = topicSentences(conf);
        conf.setInt("topicsentences", topicSentences.size());
        int count = 0;
        for (Map.Entry<Long, ArrayList<Integer>> entry : topicSentences.entrySet()) {
           conf.setLong(sprintf("topicsentence.key.%d", count), entry.getKey());
           Conf.setIntList(conf, sprintf("topicsentence.value.%d", count++), entry.getValue());
        }
    }
    
    /**
     * @param conf
     * @return map of topics, with the sentence IDs of titles that contain all 
     * query terms
     */
    public static HashMap<Long, ArrayList<Integer>> getTopicSentences(Configuration conf) throws IOException {
        HashMap<Long, ArrayList<Integer>> topicSentences = new HashMap();
        int size = conf.getInt("topicsentences", 0);
        for (int i = 0; i < size; i++) {
           long key = conf.getLong(sprintf("topicsentence.key.%d", i), 0);
           ArrayList<Integer> list = Conf.getIntList(conf, sprintf("topicsentence.value.%d", i));
           topicSentences.put(key, list);
        }
        return topicSentences;
    }
    
    /**
     * @param conf
     * @return a list of files per topic, that contain sentence IDs of titles 
     * containing all query terms, generated by TopicMatchingSentences
     * @throws IOException 
     */
    public static ArrayList<Datafile> topicFiles(Configuration conf) throws IOException {
        HDFSPath path = new HDFSPath(conf, conf.get("topics"));
        return path.getFiles("*.\\d+");
    }
        

}
