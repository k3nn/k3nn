package scrape2navigation;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.WebTools;
import io.github.repir.tools.lib.WebTools.UrlResult;
import io.github.repir.tools.hadoop.io.RandomWritable;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.ScrapeMain;
import scrape.date.SnapshotWritable;
import scrape1domain.Domain_IA;

/**
 *
 * @author jeroen
 */
public class NavigateMap extends Mapper<LongWritable, Text, RandomWritable, SnapshotWritable> {

    public static final Log log = new Log(NavigateMap.class);
    LogFile logfile;
    Domain_IA domain = Domain_IA.instance;
    SnapshotWritable outvalue = new SnapshotWritable();
    RandomWritable outkey;

    @Override
    public void setup(Context context) throws IOException {
        logfile = new LogFile(context);
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
