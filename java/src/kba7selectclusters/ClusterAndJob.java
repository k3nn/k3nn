package kba7selectclusters;

import kba7clusterand.*;
import Cluster.ClusterInputFormat;
import Cluster.ClusterWritable;
import StreamCluster.StreamClusterWritable;
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

public class ClusterAndJob {

    private static final Log log = new Log(ClusterAndJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);

        String inputfilename = conf.get("input");
        Path out = new Path(conf.get("output"));
        setQueries(conf);

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", inputfilename);
        log.info(" - output: %s", out);
        log.info(" - topics: %s", conf.get("topicfile"));

        Job job = new Job(conf, inputfilename, out, conf.get("topicfile"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

        job.setInputFormatClass(ClusterInputFormat.class);
        ClusterInputFormat.addDirs(job, inputfilename);

        job.setNumReduceTasks(10);
        job.setMapperClass(ClusterAndMap.class);
        job.setReducerClass(ClusterAndReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(IntLongWritable.class);
        job.setMapOutputValueClass(StreamClusterWritable.class);

        job.setSortComparatorClass(IntLongWritable.DecreasingComparator.class);

        FileSystem.get(conf).delete(out, true);
        return job;
    }

    public static void setQueries(Conf conf) {
        TopicFile tf = new TopicFile(new Datafile(conf.get("topicfile")));
        for (TopicWritable topic : tf) {
            conf.addArray("topicquery", topic.query);
            conf.addArray("topicid", Integer.toString(topic.id));
            conf.addArray("topicstart", Long.toString(topic.start));
            conf.addArray("topicend", Long.toString(topic.end));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
