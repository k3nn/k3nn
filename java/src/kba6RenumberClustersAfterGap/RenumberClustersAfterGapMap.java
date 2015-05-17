package kba6RenumberClustersAfterGap;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ContextTools;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class RenumberClustersAfterGapMap extends Mapper<LongWritable, ClusterNodeWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RenumberClustersAfterGapMap.class);
    ClusterNodeFile outfile;
    int increment;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        increment = conf.getInt("increment", 0);
        String name = ContextTools.getInputPath(context).getName();
        HDFSPath dir = new HDFSPath(conf, conf.get("output"));
        Datafile df = dir.getFile(name);
        outfile = new ClusterNodeFile(df);
        outfile.openWrite();
    }

    @Override
    public void map(LongWritable key, ClusterNodeWritable value, Context context) {
        if (value.clusterID > -1)
            value.clusterID += increment;
        value.write(outfile);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        outfile.closeWrite();
    }
}
