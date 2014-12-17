package scrape2.job;

import io.github.repir.tools.Content.HDFSPath;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.Configuration;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.IO.RandomWritable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import scrape.date.SnapshotOutputFormat;
import scrape.date.SnapshotWritable;
/**
 *
 * @author jeroen
 */
public class MainpageJob {
   public static final Log log = new Log( MainpageJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(args, "-i input -o output -j {jardir} -r reducers");
        conf.addLibraries(conf.getStrings("jardir", new String[0]));
        conf.setInt("yarn.app.mapreduce.am.resource.mb", 4096);
        conf.setInt("mapreduce.map.memory.mb", 2048);
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx3584m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true");
        conf.set("mapreduce.map.java.opts", "-server -Xmx1900m");
        conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.95);
        conf.setInt("mapreduce.task.timeout", 60000000);
        Job job = new Job(conf, "MainPageJob");
              
        job.setInputFormatClass(NLineInputFormat.class); 
        String input = conf.get("input");
        NLineInputFormat.addInputPath(job, new Path(input));
        NLineInputFormat.setNumLinesPerSplit(job, 100);
        job.setMapperClass(MainpageMap.class);
        job.setMapOutputKeyClass(RandomWritable.class);
        job.setMapOutputValueClass(SnapshotWritable.class);

        job.setNumReduceTasks(conf.getInt("reducers", 1000));
        job.setReducerClass(MainpageReducer.class);

        Path out = new Path(conf.get("output"));
        new SnapshotOutputFormat(job);
        SnapshotOutputFormat.setOutputPath(job, out);
        new HDFSPath(conf, out).delete();
        
        job.waitForCompletion(true);
    }
   
}
