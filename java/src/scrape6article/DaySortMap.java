package scrape6article;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchPosition;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.page.PageWritable;

/**
 *
 * @author jeroen
 */
public class DaySortMap extends Mapper<LongWritable, PageWritable, LongWritable, PageWritable> {

    public static final Log log = new Log(DaySortMap.class);
    //LogFile logfile;
    Configuration conf;
    LongWritable outkey = new LongWritable();
    long max = 0;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
    }

    @Override
    public void map(LongWritable key, PageWritable page, Context context) throws IOException, InterruptedException {
        outkey.set(page.creationtime);
        context.write(outkey, page);
    }
}
