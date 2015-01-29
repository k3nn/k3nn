package scrape3articleurl;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.lib.ArrayTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.WebTools.UrlResult;
import io.github.repir.tools.hadoop.io.RandomWritable;
import io.github.repir.tools.hadoop.io.TextLongWritable;
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
public class ArticleUrlMap extends Mapper<LongWritable, Text, TextLongWritable, SnapshotWritable> {

    public static final Log log = new Log(ArticleUrlMap.class);
    //LogMessageFile logfile;
    Domain_IA domain = Domain_IA.instance;
    SnapshotWritable outvalue = new SnapshotWritable();
    TextLongWritable outkey;

    @Override
    public void setup(Context context) throws IOException {
        //logfile = new LogMessageFile(context);
        outkey = new TextLongWritable();
    }

    @Override
    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        SnapshotWritable value = getSnapshot(text);
        if (value != null) {
            log.info("%s %s %s", value.creationtime, value.domain, value.url);
            int domainnr = domain.getDomainForHost(value.domain);
            ByteSearch articleregex = domain.getRegex(domainnr);
            log.info("regex %s", articleregex.toString());
            if (!articleregex.exists(value.url)) {
                ScrapeMain scraper = new ScrapeMain(value.url);
                UrlResult result = scraper.getResult();
                log.info("result %b", result != null);
                if (result != null) {
                    outvalue.domain = value.domain;
                    outvalue.creationtime = value.creationtime;
                    for (String hyperlink : scraper.getArticles(articleregex)) {
                        if (!ByteSearch.WHITESPACE.exists(hyperlink)) {
                            outvalue.url = hyperlink;
                            long creationtime = Long.parseLong(value.creationtime);

                            outkey.set(stripTimeLink(hyperlink), creationtime);
                            context.write(outkey, outvalue);
                            log.info("out %s %d", outvalue.url, creationtime);
                        }
                    }
                }
            }
        }
    }

    public String stripTimeLink(String hyperlink) {
        int method = hyperlink.indexOf("://");
        if (method > 0) {
            return hyperlink.substring(method + 3).trim();
        }
        return hyperlink.trim();
    }

    public SnapshotWritable getSnapshot(Text text) {
        log.info("input: %s", text.toString());
        SnapshotWritable s = null;
        String part[] = text.toString().split("\t");
        if (part.length == 3
                && part[0].length() > 0
                && part[1].length() > 0
                && part[2].length() > 0) {
            s = new SnapshotWritable();
            s.creationtime = part[0];
            s.domain = part[1];
            s.url = part[2];
        } else {
            log.info("illegal input %s", ArrayTools.toString(part));
        }
        return s;
    }
}
