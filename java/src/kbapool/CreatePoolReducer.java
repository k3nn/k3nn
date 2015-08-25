package kbapool;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import kbaeval.PoolFile;
import kbaeval.PoolWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author jeroen
 */
public class CreatePoolReducer extends Reducer<NullWritable, PoolWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(CreatePoolReducer.class);
    Configuration conf;
    PoolFile poolfile;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        poolfile = new PoolFile(new Datafile(conf, conf.get("poolfile")));
        poolfile.openWrite();
    }

    @Override
    public void reduce(NullWritable key, Iterable<PoolWritable> values, Context context) throws IOException, InterruptedException {
        for (PoolWritable p : values) {
            p.write(poolfile);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        poolfile.closeWrite();
    }
}
