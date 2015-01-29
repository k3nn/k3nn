package stream1cluster;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class StreamClusterReducer extends Reducer<IntWritable, StreamClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(StreamClusterReducer.class);
    StreamClusterFile cf;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        ArrayList<Datafile> topicFiles = StreamClusterJob.topicFiles(conf);
        HDFSPath path = new HDFSPath(conf, conf.get("output"));
        int reducer = ContextTools.getTaskID(context);
        Datafile df = path.getFile(topicFiles.get(reducer).getFilename());
        cf = new StreamClusterFile(df);
    }

    @Override
    public void reduce(IntWritable key, Iterable<StreamClusterWritable> values, Context context) throws IOException, InterruptedException {
        for (StreamClusterWritable value : values) {
            log.info("%d %d", value.clusterid, value.urls.get(value.urls.size() - 1).urlid);
            value.write(cf);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        cf.closeWrite();
    }
}
