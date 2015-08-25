package secondary1docs;

import Cluster.ClusterInputFormat;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.io.HDFSPath;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import Sentence.SentenceInputFormat;

public class RelevantDocsJob {

    private static final Log log = new Log(RelevantDocsJob.class);

    public static Job setup(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output");

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);

        Job job = new Job(conf, input, out);
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
        
        job.setInputFormatClass(ClusterInputFormat.class);
        SentenceInputFormat.addDirs(job, input);

        job.setNumReduceTasks(0);
        job.setMapperClass(RelevantDocsMap.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        DocOutputFormat.setSingleOutput(job, out);

        FileSystem.get(conf).delete(out, true);
        return job;
    }
    
    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
