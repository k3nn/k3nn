package kba2sentences_ia;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.HtmlTitleExtractor;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.type.Tuple2;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kba1raw.Domain_KBA;
import kba1raw.ReducerKeysDays;
import kba1raw.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import scrape.page.PageWritable;
import streamcorpus.ContentItem;
import streamcorpus.StreamItem;
import streamcorpus.sentence.SentenceFile;

/**
 *
 * @author jeroen
 */
public class SentenceIAMap extends Mapper<LongWritable, PageWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(SentenceIAMap.class);

    SentenceWritable outvalue = new SentenceWritable();
    Domain_KBA domainfilter = new Domain_KBA();
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
    }

    @Override
    public void map(LongWritable key, PageWritable value, Context context) throws IOException, InterruptedException {
        outvalue.domain = domainfilter.getDomainForUrl(value.url);
        if (outvalue.domain >= 0) {
            outvalue.creationtime = value.creationtime;
            outvalue.sentence = this.getTitle(value.url, value.content);
            if (outvalue.sentence != null) {
                outvalue.row = 0;
                outvalue.idhigh = 0;
                outvalue.idlow = 0;
                int day = ReducerKeysDays.getDay(value.creationtime);
                if (day >= 0) {
                    outvalue.id = day << 22;
                    int hashKey = MathTools.hashCode(outvalue.domain, outvalue.sentence.hashCode());
                    outkey.set(hashKey, value.creationtime);
                    context.write(outkey, outvalue);
                }
            }
        }
    }

    HtmlTitleExtractor extractor = new HtmlTitleExtractor();

    public String getTitle(String url, byte[] content) {
        ArrayList<String> title = extractor.extract(content);
        if (title != null && title.size() > 0) {
            return TitleFilter.filter(url, title.get(0));
        }
        return null;
    }

}
