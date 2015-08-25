package stream2AddDocID;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.io.IntPartitioner;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;

/**
 * Retrieves the docIDs for the query matching clusters
 * @author jeroen
 */
public class ClusterDocidJob {

    private static final Log log = new Log(ClusterDocidJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -r result -t topicfile");
        conf.setReduceSpeculativeExecution(false);
        conf.setTaskTimeout(60000000);
        conf.setReduceMemoryMB(4096);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));
        setArticles(conf);

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - result dir: %s", conf.get("result"));
        log.info(" - topicfile: %s", conf.get("topicfile"));

        Job job = new Job(conf, input, out);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);

        job.setNumReduceTasks(getResultFiles(conf).size());
        job.setMapperClass(ClusterDocidMap.class);
        job.setReducerClass(ClusterDocidReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setPartitionerClass(IntPartitioner.class);

        FileSystem.get(conf).delete(out, true);
        return job;
    }

    /**
     * Stores a list of nodeID's for which docid's have to be retrieved
     * in the configuration
     * @param conf
     */
    public static void setArticles(Conf conf) throws IOException {
        ArrayList<Datafile> files = getResultFiles(conf);
        for (int i = 0; i < files.size(); i++) {
            files.get(i).setBufferSize(100000000);
            ClusterFile cf = new ClusterFile(files.get(i));
            HashSet<Long> list = new HashSet();
            for (ClusterWritable cluster : cf) {
                log.info("%d", cluster.clusterid);
                for (NodeWritable node : cluster.nodes)
                   list.add(node.sentenceID);
            }
            conf.setLongList("resultids." + i, list);
        }
        conf.setInt("topics", files.size());
    }
    
    /**
     * @param conf
     * @return a map of reducer ID, list of nodeIDs for which docIDs are retrieved
     */
    public static HashMap<Long, ArrayList<Integer>> getArticles(Conf conf) {
        HashMap<Long, ArrayList<Integer>> map = new HashMap();
        int topics = conf.getInt("topics", 0);
        for (int i = 0; i < topics; i++) {
            ArrayList<Long> longList = conf.getLongList("resultids." + i);
            for (long j : longList) {
                ArrayList<Integer> list = map.get(j);
                if (list == null) {
                    list = new ArrayList();
                    map.put(j, list);
                }
                list.add(i);
            }
        }
        return map;
    }
    
    /**
     * @param conf
     * @return list of result files, for which docIDs must be retrieved
     */
    public static ArrayList<Datafile> getResultFiles(Conf conf) throws IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("result"));
        return dir.getFiles();
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
