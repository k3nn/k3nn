package kba8docid;

import Cluster.ClusterFile;
import Cluster.ClusterInputFormat;
import Cluster.ClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntPartitioner;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;
import streamcorpus.sentence.SentenceWritable;

public class ClusterDocidJob {

    private static final Log log = new Log(ClusterDocidJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -r result -t topicfile");
        conf.setReduceSpeculativeExecution(false);

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

    public static void setArticles(Conf conf) throws IOException {
        ArrayList<Datafile> files = getResultFiles(conf);
        for (int i = 0; i < files.size(); i++) {
            ClusterFile cf = new ClusterFile(files.get(i));
            ArrayList<Integer> list = new ArrayList();
            for (ClusterWritable article : cf) {
                list.add(article.urlid);
            }
            conf.setIntList("resultids." + i, list);
        }
        conf.setInt("topics", files.size());
    }
    
    public static HashMap<Integer, ArrayList<Integer>> getArticles(Conf conf) {
        HashMap<Integer, ArrayList<Integer>> map = new HashMap();
        int topics = conf.getInt("topics", 0);
        for (int i = 0; i < topics; i++) {
            ArrayList<Integer> intList = conf.getIntList("resultids." + i);
            for (int j : intList) {
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
    
    public static ArrayList<Datafile> getResultFiles(Conf conf) throws IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("result"));
        return dir.getFiles();
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
