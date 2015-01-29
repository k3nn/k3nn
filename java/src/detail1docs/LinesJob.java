package detail1docs;

import kba8docid.*;
import Cluster.ClusterFile;
import Cluster.ClusterInputFormat;
import Cluster.ClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongWritable;
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

public class LinesJob {

    private static final Log log = new Log(LinesJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -r result");
        conf.setReduceSpeculativeExecution(false);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - result dir: %s", conf.get("result"));

        Job job = new Job(conf, input, out);

        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addDirs(job, input);
        SentenceInputFormat.setNonSplitable(job);

        job.setNumReduceTasks(getResultFiles(conf).size());
        job.setMapperClass(LinesMap.class);
        job.setReducerClass(LinesReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setPartitionerClass(IntPartitioner.class);
        job.setSortComparatorClass(IntLongWritable.SortComparator.class);

        return job;
    }

    public static HashMap<Integer, ArrayList<Integer>> mapDocReducer(Conf conf) throws IOException {
        HashMap<Integer, ArrayList<Integer>> result = new HashMap();
        ArrayList<Datafile> resultFiles = getResultFiles(conf);
        for (int reducer = 0; reducer < resultFiles.size(); reducer++) {
            ClusterFile cf = new ClusterFile(resultFiles.get(reducer));
            for (ClusterWritable w : cf) {
                ArrayList<Integer> reducers = result.get(w.urlid);
                if (reducers == null) {
                    reducers = new ArrayList();
                    result.put(w.urlid, reducers);
                }
                reducers.add(reducer);
            }
        }
        return result;
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
