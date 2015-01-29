package scrape3articleurl;

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
public class ArticleUrlReducer extends Reducer<TextLongWritable, SnapshotWritable, NullWritable, SnapshotWritable> {

    public static final Log log = new Log(ArticleUrlReducer.class);

    @Override
    public void reduce(TextLongWritable key, Iterable<SnapshotWritable> values, Context context) throws IOException, InterruptedException {
        SnapshotWritable value = values.iterator().next();
        context.write(NullWritable.get(), value);
        log.info("write %s %s", value.url, value.creationtime);
        for (SnapshotWritable s : values) {
            log.info("skip %s %s", value.url, value.creationtime);
        }
    }
}
