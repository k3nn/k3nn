package ECterms;

import ECterms.ECCountJob.Topic;
import KNN.Stream;
import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMapList;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import kbaeval.TopicWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Reads KBA StreamItems, and writes the contents of pages from NewsDomains as
 * SentenceWritables.
 *
 * @author jeroen
 */
public class ECCountMap extends Mapper<LongWritable, SentenceWritable, IntTextInt, IntWritable> {

    public static final Log log = new Log(ECCountMap.class);
    static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    IntTextInt outkey = new IntTextInt();
    IntWritable outvalue = new IntWritable(1);
    HashSet<String> allterms;
    HashMapList<String, Topic> map;
    HashSet<String> doc;
    String currentdocument = "";

    @Override
    public void setup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        ArrayList<Topic> topics = ECCountJob.getTopics(conf);
        map = new HashMapList();
        for (Topic t : topics) {
            for (String term : t.terms) {
                map.add(term, t);
                allterms.add(term);
            }
        }
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        if (currentdocument.equals(value.getDocumentID())) {
            cleanup(context);
            doc = new HashSet();
            currentdocument = value.getDocumentID();
        }
        for (String term : tokenizer.tokenize(value.content)) {
            doc.add(term);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        HashSet<Topic> topics = new HashSet();
        for (String term : allterms) {
            if (doc.contains(term)) {
                ArrayList<Topic> list = map.get(term);
                if (list != null) {
                    topics.addAll(list);
                }
            }
        }
        for (Topic t : topics) {
            int termspresent = 0;
            for (int i = 0; i < t.terms.size(); i++) {
                if (doc.contains(t.terms.get(i))) {
                    termspresent |= 1 << i;
                }
            }
            outkey.set(t.id);
            outkey.value2 = termspresent;
            for (String term : doc) {
                outkey.value1 = term;
                context.write(outkey, outvalue);
            }
        }
    }
}
