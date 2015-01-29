package kba6finalcluster;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.IntIntWritable;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class FinalReducer extends Reducer<IntIntWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(FinalReducer.class);
    ClusterFile cf;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        HDFSPath path = new HDFSPath(conf, conf.get("output"));
        ArrayList<String> dates = FinalMap.getDates(conf);
        Datafile df = path.getFile(dates.get(ContextTools.getTaskID(context)));
        cf = new ClusterFile(df);
        cf.openWrite();
    }

    @Override
    public void reduce(IntIntWritable key, Iterable<ClusterWritable> values, Context context) throws IOException, InterruptedException {
        for (ClusterWritable w : values) 
            w.write(cf);
    }
    
    @Override
    public void cleanup(Context context) {
        cf.closeWrite();
    }
}
