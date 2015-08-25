package stream1ClusterTitles;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.ContextTools;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write the cluster snapshots to a file per topic.
 * @author jeroen
 */
public class ClusterTitlesReducer extends Reducer<IntWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterTitlesReducer.class);
    ClusterFile cf;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        ArrayList<Datafile> topicFiles = ClusterTitlesJob.topicFiles(conf);
        HDFSPath path = new HDFSPath(conf, conf.get("output"));
        int reducer = ContextTools.getTaskID(context);
        Datafile df = path.getFile(topicFiles.get(reducer).getName());
        cf = new ClusterFile(df);
    }

    @Override
    public void reduce(IntWritable key, Iterable<ClusterWritable> values, Context context) throws IOException, InterruptedException {
        for (ClusterWritable value : values) {
            value.write(cf);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        cf.closeWrite();
    }
}
