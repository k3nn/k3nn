package secondary2clustersbacklinks;

import secondary2clusters.*;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.io.LongLongWritable;
import java.io.IOException;
import java.util.HashMap;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import Sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterMap extends Mapper<LongWritable, SentenceWritable, LongLongWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterMap.class);
    Configuration conf;
    NewsDomains domain = NewsDomains.instance;
    HashMap<String, Long> relevantdocs;
    LongLongWritable outkey = new LongLongWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        relevantdocs = ClusterJob.getRelevantDocs(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        //log.info("%d %s", value.id, value.getDocumentID());
        Long emittime = relevantdocs.get(value.getDocumentID());
        if (emittime != null) {
            if (value.sentenceNumber != 0) { // row 0 is duplicate for extracted title -1
                if (value.sentenceNumber == -1) {
                    value.sentenceNumber = 0;
                    String dom = domain.getHost(value.domain);
                    value.content = TitleFilter.filterHost(dom, value.content);
                }
                outkey.set(emittime, value.creationtime);
                context.write(outkey, value);
            }
        }
    }
}
