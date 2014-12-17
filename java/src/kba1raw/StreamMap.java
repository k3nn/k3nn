package kba1raw;

import io.github.repir.tools.Extractor.HtmlTitleExtractor;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.IO.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.LogMessageFile;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import streamcorpus.ContentItem;
import streamcorpus.Sentence;
import streamcorpus.StreamItem;
import streamcorpus.Token;

/**
 *
 * @author jeroen
 */
public class StreamMap extends Mapper<LongWritable, StreamItem, IntLongStringIntWritable, SentenceWritable> {

    public static final Log log = new Log(StreamMap.class);
    LogMessageFile logfile;
    SentenceWritable outvalue = new SentenceWritable();
    Domain_KBA domainfilter = new Domain_KBA();
    IntLongStringIntWritable outkey = new IntLongStringIntWritable();
    ReducerKeysDays reducerkeys;

    @Override
    public void setup(Context context) throws IOException {
        logfile = new LogMessageFile(context);
        reducerkeys = new ReducerKeysDays(context.getConfiguration());
    }

    @Override
    public void map(LongWritable key, StreamItem value, Context context) throws IOException, InterruptedException {
        String url = getUrl(value);
        outvalue.domain = domainfilter.getDomainForUrl(url);
        if (outvalue.domain >= 0) {
            try {
                String title = getTitle(url, value);
                if (title != null) {
                    //log.info("%s %d %s", value.stream_id, outvalue.domain, url);
                    outvalue.creationtime = creationTime(value);
                    UUID docid = readID(value);
                    outvalue.idlow = docid.getLeastSignificantBits();
                    outvalue.idhigh = docid.getMostSignificantBits();
                    int day = reducerkeys.getDay(value);
                    outvalue.id = day << 22;
                    log.info("url %s", url);
                    log.info("Title %s", title);
                    outkey.set(0, outvalue.creationtime, value.getDoc_id(), 0);
                    outvalue.row = 0;
                    outvalue.sentence = title;
                    context.write(outkey, outvalue);
                }
            } catch (ParseException ex) {
                log.exception(ex);
            }
        } else {
            //log.info("no domain %s", value.stream_id);
        }
    }

    public static UUID readID(StreamItem item) {
        String name = item.getDoc_id();
        if (name.length() != 32) {
            throw new IllegalArgumentException("Invalid UUID string: " + name);
        }

        long mostSigBits = Long.valueOf(name.substring(0, 8), 16);
        mostSigBits <<= 32;
        mostSigBits |= Long.valueOf(name.substring(8, 16), 16);

        long leastSigBits = Long.valueOf(name.substring(16, 24), 16);
        leastSigBits <<= 32;
        leastSigBits |= Long.valueOf(name.substring(24), 16);

        UUID uuid = new UUID(mostSigBits, leastSigBits);

        return uuid;
    }

    public String getUrl(StreamItem i) {
        if (i.isSetAbs_url()) {
            return new String(i.getAbs_url());
        }
        return "";
    }

    HtmlTitleExtractor extractor = new HtmlTitleExtractor();

    public String getTitle(String url, StreamItem i) {
        ArrayList<String> result = new ArrayList();
        ContentItem body = i.getBody();
        if (body != null) {
            String b = body.getClean_html();
            if (b != null) {
                //log.info("%s", b);
                ArrayList<String> title = extractor.extract(b.getBytes());
                //log.info("title %s", title);
                if (title != null && title.size() > 0) {
                    return TitleFilter.filter(url, title.get(0));
                }
            }
        }
        return null;
    }

    public long creationTime(StreamItem i) throws ParseException {
        return (long) i.getStream_time().getEpoch_ticks();
    }
}
