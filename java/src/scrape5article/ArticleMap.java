package scrape5article;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.UrlStrTools;
import io.github.repir.tools.lib.WebTools.UrlResult;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.FetchPage;
import scrape.date.SnapshotWritable;
import scrape.page.PageWritable;
import scrape1domain.Domain_IA;

/**
 *
 * @author jeroen
 */
public class ArticleMap extends Mapper<LongWritable, Text, NullWritable, PageWritable> {

    public static final Log log = new Log(ArticleMap.class);
    //LogFile logfile;
    Configuration conf;
    ByteSearch creationtime = ByteSearch.create("\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d");
    ByteSearch html = ByteSearch.create("<html(\\s|>)");
    ByteSearch head = ByteSearch.create("<head(\\s|>)");
    ByteSearch title = ByteSearch.create("<title(\\s|>)");
    Domain_IA domain = Domain_IA.instance;
    PageWritable outvalue = new PageWritable();
    LongWritable outkey;

    public enum COUNTERS {

        DATEOUTSIDE,
        NOPAGE,
        INVALIDREDIRECT,
        WRONGMIME,
        CORRECT
    }

    @Override
    public void setup(Context context) throws IOException {
        //logfile = new LogFile(context);
        conf = context.getConfiguration();
        outkey = new LongWritable();
    }

    @Override
    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        SnapshotWritable value = getSnapshot(text);
        log.info("%s %s %s", value.creationtime, value.domain, value.url);
        try {
            Date parse = DateTools.FORMAT.YMD.parse(value.creationtime.substring(0, 8));
            outkey.set(parse.getTime() / 1000);
            if (DayPartitioner.validDate(conf, outkey.get())) {
                UrlResult result = FetchPage.fetchPage(value.url);
                if (result != null) {
                    boolean ishtml = html.exists(result.content);
                    boolean ishead = ishtml && head.exists(result.content);
                    boolean istitle = ishead && title.exists(result.content);
                    //log.info("MIME %b %b %b %s", ishtml, ishead, istitle, result.redirected.toString());
                    if (ishtml && ishead && istitle) {
                        outvalue.url = UrlStrTools.stripHost(result.redirected.toString());
                        String time = creationtime.extract(outvalue.url);
                        log.info("have result %s %s", outvalue.url, time);
                        if (time != null) {
                            parse = DateTools.FORMAT.YMD.parse(time.substring(0, 8));
                            outkey.set(parse.getTime() / 1000);
                            if (DayPartitioner.validDate(conf, outkey.get())) {
                                //logfile.write("out %s %s", time, outvalue.url);
                                context.getCounter(COUNTERS.CORRECT).increment(1);
                                outvalue.creationtime = outkey.get();
                                // -- change!!!! outvalue.content = result.content;
                                context.write(NullWritable.get(), outvalue);
                            } else {
                                context.getCounter(COUNTERS.DATEOUTSIDE).increment(1);
                            }
                        } else {
                            context.getCounter(COUNTERS.INVALIDREDIRECT).increment(1);
                        }
                    } else {
                        context.getCounter(COUNTERS.WRONGMIME).increment(1);
                    }
                } else {
                    context.getCounter(COUNTERS.NOPAGE).increment(1);
                }
            } else {
                context.getCounter(COUNTERS.DATEOUTSIDE).increment(1);
            }
        } catch (ParseException ex) {
            log.exception(ex, "date %s", value.creationtime);
        }
    }

    public SnapshotWritable getSnapshot(Text text) {
        log.info("input: %s", text.toString());
        String part[] = text.toString().split("\t");
        SnapshotWritable s = new SnapshotWritable();
        s.creationtime = part[0];
        s.domain = part[1];
        s.url = part[2];
        return s;
    }
}
