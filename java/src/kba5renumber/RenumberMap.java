package kba5renumber;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
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
public class RenumberMap extends Mapper<LongWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RenumberMap.class);
    ClusterFile outfile;
    int increment;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        increment = conf.getInt("increment", 0);
        String name = ContextTools.getInputPath(context).getName();
        HDFSPath dir = new HDFSPath(conf, conf.get("output"));
        Datafile df = dir.getFile(name);
        outfile = new ClusterFile(df);
        outfile.openWrite();
    }

    @Override
    public void map(LongWritable key, ClusterWritable value, Context context) {
        if (value.clusterid > -1)
            value.clusterid += increment;
        value.write(outfile);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        outfile.closeWrite();
    }
}
