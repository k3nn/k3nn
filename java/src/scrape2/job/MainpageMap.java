package scrape2.job;

import io.github.repir.tools.ByteSearch.ByteSearch;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.WebTools;
import io.github.repir.tools.Lib.WebTools.UrlResult;
import io.github.repir.tools.hadoop.IO.RandomWritable;
import io.github.repir.tools.hadoop.LogMessageFile;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.ScrapeMain;
import scrape.date.SnapshotWritable;
import scrape1.job.Domain_IA;

/**
 *
 * @author jeroen
 */
public class MainpageMap extends Mapper<LongWritable, Text, RandomWritable, SnapshotWritable> {

    public static final Log log = new Log(MainpageMap.class);
    LogMessageFile logfile;
    Domain_IA domain = Domain_IA.instance;
    SnapshotWritable outvalue = new SnapshotWritable();
    RandomWritable outkey;

    @Override
    public void setup(Context context) throws IOException {
        logfile = new LogMessageFile(context);
        outkey = new RandomWritable(context);
    }

    @Override
    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        SnapshotWritable value = getSnapshot(text);
        log.info("%s %s %s", value.creationtime, value.domain, value.url);
        int domainnr = domain.getDomainForHost(value.domain);
        ByteSearch articleregex = domain.getRegex(domainnr);
        ScrapeMain scraper = new ScrapeMain(value.url);
        UrlResult result = scraper.getResult();
        if (result != null) {
            outvalue.url = result.redirected.getPath();
            outvalue.domain = value.domain;
            outvalue.creationtime = value.creationtime;
            outkey.generateKey();
            context.write(outkey, outvalue);
            logfile.write("%s", outvalue.url);
            for (String hyperlink : scraper.getNonArticles(articleregex)) {
                outvalue.url = hyperlink;
                outkey.generateKey();
                context.write(outkey, outvalue);
                logfile.write("%s", outvalue.url);
            }
        }
    }

    public SnapshotWritable getSnapshot(Text text) {
        String part[] = text.toString().split("\t");
        SnapshotWritable s = new SnapshotWritable();
        s.creationtime = part[0];
        s.domain = part[1];
        s.url = part[2];
        return s;
    }
}
