package CreateIDF;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import io.github.repir.EntityReader.EntityReaderWikipedia;
import io.github.repir.EntityReader.MapReduce.EntityReaderInputFormat;
import org.apache.hadoop.io.Text;

public class CreateIDFJob {

    private static final Log log = new Log(CreateIDFJob.class);

    public static void main(String[] args) throws Exception {

        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(2048);
        conf.setTaskTimeout(6000000);
        conf.setMapSpeculativeExecution(true);
        conf.setReduceSpeculativeExecution(false);
        conf.set("repository.inputdir", conf.get("input"));
        //conf.setBoolean("repository.testinputformat", true);
        conf.set("repository.entityreader", EntityReaderWikipedia.class.getSimpleName());
        conf.setBoolean("repository.splitablesource", true);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        Job job = new Job(conf, input, out);

        new EntityReaderInputFormat(job);

        job.setNumReduceTasks(1);
        job.setMapperClass(CreateIDFMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setReducerClass(CreateIDFReducer.class);
        
        job.waitForCompletion(true);
    }
}
