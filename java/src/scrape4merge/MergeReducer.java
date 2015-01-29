package scrape4merge;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.TextLongWritable;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import scrape.date.SnapshotWritable;

/**
 *
 * @author jeroen
 */
public class MergeReducer extends Reducer<TextLongWritable, SnapshotWritable, NullWritable, SnapshotWritable> {

    public static final Log log = new Log(MergeReducer.class);

    @Override
    public void reduce(TextLongWritable key, Iterable<SnapshotWritable> values, Context context) throws IOException, InterruptedException {
        // all entries for the exeact same URL are grouped and sorted desc, so we only keep the first
        // even if that page is not available in the Internet Archive, it should then
        // retrieve the next one available in time.
        SnapshotWritable value = values.iterator().next();
        context.write(NullWritable.get(), value);
    }
}
