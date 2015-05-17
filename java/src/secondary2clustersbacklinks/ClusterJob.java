package secondary2clustersbacklinks;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.LongLongWritable;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.DateTools;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import secondary1docs.DocFile;
import secondary1docs.DocWritable;
import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;

public class ClusterJob {

    private static final Log log = new Log(ClusterJob.class);
    static Conf conf;

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -d source");
        conf.setReduceSpeculativeExecution(false);
        conf.setReduceMemoryMB(4096);
        getRelevantDocs(conf);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - source: %s", conf.get("source"));

        Job job = new Job(conf, input, out, conf.get("source"));
                //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
        
        job.setInputFormatClass(SentenceInputFormat.class);
        setInputFiles(job);

        job.setNumReduceTasks(1);
        job.setMapperClass(ClusterMap.class);
        job.setReducerClass(ClusterReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(LongLongWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);

        job.setSortComparatorClass(LongLongWritable.SortComparator.class);

        return job;
    }

    public static HashMap<String, Long> getRelevantDocs(Configuration conf) {
        HashMap<String, Long> result = new HashMap();
        Datafile df = new Datafile(conf, conf.get(("input")));
        DocFile docfile = new DocFile(df);
        for (DocWritable d : docfile) {
            result.put(d.docid, d.emit);
        }
        return result;
    }
    
    public static void setInputFiles(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        HashSet<String> dates = new HashSet();
        HashMap<String, Long> relevantDocs = getRelevantDocs(conf);
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
