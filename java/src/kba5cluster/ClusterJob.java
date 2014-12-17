package kba5cluster;

import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import streamcorpus.sentence.SentenceInputFormat;

public class ClusterJob {

	private static final Log log = new Log(ClusterJob.class);

	public static void main(String[] args) throws Exception {

                Configuration conf = new Configuration(args, 
                                "-i input -o output -j {jardir}");
                conf.addLibraries(conf.getStrings("jardir", new String[0]));
                
                String inputfilename = conf.get("input");
                Path out = new Path(conf.get("output"));

		log.info("Tool name: %s", log.getLoggedClass().getName());
		log.info(" - input webpages paths: %s", inputfilename);
		log.info(" - output clickedurls path: %s", out);

		Job job = new Job(conf, "%s [%s] [%s]", log.getLoggedClass().getName(), inputfilename, out);
                job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
                job.setJarByClass(log.getLoggedClass());
            
		job.setInputFormatClass(SentenceInputFormat.class);
                SentenceInputFormat.addDirs(job, inputfilename);
                SentenceInputFormat.setNonSplitable(job);
                
		//job.setNumReduceTasks(0);
		job.setMapperClass(ClusterMap.class);
                job.setOutputFormatClass(NullOutputFormat.class);

		job.waitForCompletion(true);
	}
}
