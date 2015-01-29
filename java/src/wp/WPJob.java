package wp;

import streamcorpus.sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.document.DocumentInputFormat;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
/**
 *
 * @author jeroen
 */
public class WPJob {
   public static final Log log = new Log( WPJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(4096);
        conf.setReduceSpeculativeExecution(false);
        
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        String input = conf.get("input");
        HDFSPath in = new HDFSPath(conf, input);
        job.setInputFormatClass(DocumentInputFormat.class);
        DocumentInputFormat.setDocumentStart(conf, "<page>");
        DocumentInputFormat.setDocumentEnd(conf, "</page>");
        DocumentInputFormat.addDirs(job, in);
        
        job.setMapperClass(WPMap.class);
        job.setMapOutputKeyClass(LinkWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(1);
        job.setReducerClass(WPReducer.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        Path out = new Path(conf.get("output"));
        new LinkOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        //new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
