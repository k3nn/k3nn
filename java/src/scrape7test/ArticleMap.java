package scrape7test;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.page.PageFile;
import scrape.page.PageWritable;

/**
 *
 * @author jeroen
 */
public class ArticleMap extends Mapper<LongWritable, PageWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ArticleMap.class);
    PageFile pf;

    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        Datafile df = new Datafile(conf, conf.get("output"));
        pf = new PageFile(df);
        pf.openWrite();
    }
    
    @Override
    public void map(LongWritable key, PageWritable page, Context context) throws IOException, InterruptedException {
        page.write(pf);
    }
    
    public void cleanup(Context context) {
        pf.closeWrite();
    }
}
