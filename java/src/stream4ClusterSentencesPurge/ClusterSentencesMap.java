package stream4ClusterSentencesPurge;

import KNN.Stream;
import static KNN.Stream.getUnstemmedTokenizer;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import kba1SourceToSentences.NewsDomains;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMap3;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.hadoop.io.LongBoolWritable;
import io.github.repir.tools.type.KV;
import kba1SourceToSentences.TitleFilter;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesMap extends Mapper<LongWritable, SentenceWritable, LongBoolWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterSentencesMap.class);
    Configuration conf;
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    NewsDomains domain = NewsDomains.instance;
    HashMap3<String, Long, Boolean> relevantdocs;
    LongBoolWritable outkey = new LongBoolWritable();

    enum Counter {
        candidate,
        noncandidate
    }

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        relevantdocs = ClusterSentencesJob.getRelevantDocs(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        KV<Long, Boolean> docparams = relevantdocs.get(value.getDocumentID());
        if (docparams != null) {
            if (value.sentenceNumber != 0) { // row 0 is duplicate for extracted title -1
                if (value.sentenceNumber == -1) {
                    value.sentenceNumber = 0;
                    String dom = domain.getHost(value.domain);
                    value.content = TitleFilter.filterHost(dom, value.content);
                }
                    outkey.set(value.sentenceID, docparams.value);
                    context.write(outkey, value);
                    if (docparams.value) {
                        context.getCounter(Counter.candidate).increment(1);
                    } else {
                        context.getCounter(Counter.noncandidate).increment(1);
                    }
            }
        }
    }
}
