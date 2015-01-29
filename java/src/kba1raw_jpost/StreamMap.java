package kba1raw_jpost;

import io.github.repir.tools.extract.HtmlTitleExtractor;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import kba1raw.Domain_KBA;
import kba1raw.TitleFilter;
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
    LogFile logfile;
    SentenceWritable outvalue = new SentenceWritable();
    Domain_JPost domainfilter = new Domain_JPost();
    Domain_KBA domainfilter2 = new Domain_KBA();
    IntLongStringIntWritable outkey = new IntLongStringIntWritable();
    ReducerKeysDays reducerkeys;

    @Override
    public void setup(Context context) throws IOException {
        logfile = new LogFile(context);
        reducerkeys = new ReducerKeysDays(context.getConfiguration());
    }

    @Override
    public void map(LongWritable key, StreamItem value, Context context) throws IOException, InterruptedException {
        String url = getUrl(value);
        outvalue.domain = domainfilter2.getDomainForUrl(url);
        if (domainfilter.getDomainForUrl(url) >= 0 && outvalue.domain >= 0) {
            try {
                //log.info("%s %d %s", value.stream_id, outvalue.domain, url);
                outvalue.creationtime = creationTime(value);
                UUID docid = readID(value);
                outvalue.idlow = docid.getLeastSignificantBits();
                outvalue.idhigh = docid.getMostSignificantBits();
                int day = reducerkeys.getDay(value);
                outvalue.id = day << 22;
                String title = getTitle(url, value);
                if (title != null) {
                    outkey.set(0, outvalue.creationtime, value.getDoc_id(), -1);
                    outvalue.row = -1;
                    outvalue.sentence = title;
                    context.write(outkey, outvalue);
                }
                ArrayList<String> sentences = getSentences(value);
                for (int row = 0; row < sentences.size(); row++) {
                    //log.info("Sentence %d %s", row, sentences.getUUID(row));
                    outkey.set(0, outvalue.creationtime, value.getDoc_id(), row);
                    outvalue.row = row;
                    outvalue.sentence = sentences.get(row);
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
    
    public ArrayList<String> getSentences(StreamItem i) {
        ArrayList<String> result = new ArrayList();
        ContentItem body = i.getBody();
        if (body != null) {
            Map<String, List<Sentence>> sentences = body.getSentences();
            for (Map.Entry<String, List<Sentence>> entry : sentences.entrySet()) {
                for (Sentence s : entry.getValue()) {
                    StringBuilder sb = new StringBuilder();
                    for (Token t : s.getTokens()) {
                        sb.append(" ").append(t.getToken());
                    }
                    if (sb.length() > 0) {
                        result.add(sb.deleteCharAt(0).toString());
                    }
                }
            }
        }
        return result;
    }

    public long creationTime(StreamItem i) throws ParseException {
        return (long) i.getStream_time().getEpoch_ticks();
    }
}
