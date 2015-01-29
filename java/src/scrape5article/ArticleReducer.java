package scrape5article;

import scrape5article.*;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import scrape.page.PageWritable;

/**
 *
 * @author jeroen
 */
public class ArticleReducer extends Reducer<NullWritable, PageWritable, NullWritable, PageWritable> {

    public static final Log log = new Log(ArticleReducer.class);
    
    @Override
    public void reduce(NullWritable key, Iterable<PageWritable> values, Context context) throws IOException, InterruptedException {
        for (PageWritable s : values) {
            context.write(NullWritable.get(), s);
        }
    }
}
