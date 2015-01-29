package scrape6article;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import scrape.page.PageFile;
import scrape.page.PageWritable;

/**
 *
 * @author jeroen
 */
public class DaySortReducer extends Reducer<LongWritable, PageWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(DaySortReducer.class);
    Configuration conf;
    PageFile pagefile;
    
    @Override
    public void setup(Context context) {
        conf = context.getConfiguration();
        int taskID = ContextTools.getTaskID(context);
        String date = DayPartitioner.getDate(conf, taskID);
        log.info("date %s", date);
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        Datafile df = outdir.getFile(date);
        pagefile = new PageFile(df);
        pagefile.openWrite();
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<PageWritable> values, Context context) throws IOException, InterruptedException {
        for (PageWritable s : values) {
            log.info("%s", s.url);
            s.write(pagefile);
        }
    }
    
    @Override
    public void cleanup(Context context) {
        pagefile.closeWrite();
    }
}
