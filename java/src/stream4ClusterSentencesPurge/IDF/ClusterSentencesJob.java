package stream4ClusterSentencesPurge.IDF;

import stream4ClusterSentencesPurge.*;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.InputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.DateTools;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import stream3DocStream.DocFile;
import stream3DocStream.DocWritable;
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;
import io.github.htools.collection.HashMap3;
import io.github.htools.hadoop.io.LongBoolWritable;
import org.apache.hadoop.io.LongWritable;

public class ClusterSentencesJob {

    private static final Log log = new Log(ClusterSentencesJob.class);
    static Conf conf;

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -d source -f idf");
        conf.setReduceSpeculativeExecution(false);
        conf.setTaskTimeout(100 * 6 * 60 * 60);
        conf.setReduceMemoryMB(8192);
        getRelevantDocs(conf);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - source: %s", conf.get("source"));

        Job job = new Job(conf, input, out, conf.get("source"), conf.get("idf"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
        
        job.setInputFormatClass(SentenceInputFormat.class);
        setInputFiles(job);

        job.setNumReduceTasks(1);
        job.setMapperClass(ClusterSentencesMap.class);
        job.setReducerClass(ClusterSentencesReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(LongBoolWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        job.setSortComparatorClass(LongWritable.Comparator.class);

        return job;
    }

    public static HashMap3<String, Long, Boolean> getRelevantDocs(Configuration conf) {
        HashMap3<String, Long, Boolean> result = new HashMap3();
        Datafile df = new Datafile(conf, conf.get(("input")));
        DocFile docfile = new DocFile(df);
        for (DocWritable d : docfile) {
            result.put(d.docid, d.creationtime, d.isCandidate);
        }
        return result;
    }
    
    public static void setInputFiles(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        HashSet<String> dates = new HashSet();
        HashMap3<String, Long, Boolean> relevantDocs = getRelevantDocs(conf);
        for (String docid : relevantDocs.keySet()) {
            String part[] = docid.split("-");
            if (part.length == 2) {
                long creationtime = Long.parseLong(part[0]);
                Date creationdate = DateTools.epochToDate(creationtime);
                dates.add(DateTools.FORMAT.Y_M_D.format(creationdate));
            }
        }
        
        HDFSPath inpath = new HDFSPath(conf, conf.get("source"));
        for (String date : dates) {
            InputFormat.addDirs(job, inpath.getFilename(date));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
