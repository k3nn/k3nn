package scrape2.job;

import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.IO.RandomWritable;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import scrape.date.SnapshotWritable;

/**
 *
 * @author jeroen
 */
public class MainpageReducer extends Reducer<RandomWritable, SnapshotWritable, NullWritable, SnapshotWritable> {

    public static final Log log = new Log(MainpageReducer.class);

    @Override
    public void reduce(RandomWritable key, Iterable<SnapshotWritable> values, Context context) throws IOException, InterruptedException {
        for (SnapshotWritable value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
