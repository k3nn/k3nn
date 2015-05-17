package CreateIDF;

import KNN.Stream;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.extract.Content;
import io.github.repir.tools.extract.WikipediaSourceSplitter;
import io.github.repir.tools.extract.WikipediaSourceSplitter.Result;
import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchPosition;
import org.apache.hadoop.io.Text;

/**
 * Clusters the titles of one single day, starting with the clustering results
 * at the end of yesterday,
 *
 * @author jeroen
 */
public class CreateIDFMap extends Mapper<LongWritable, Content, Text, IntWritable> {

    public static final Log log = new Log(CreateIDFMap.class);
    // tokenizes on non-alphanumeric characters, lowercase, stop words removed, no stemming
    WikipediaSourceSplitter wpsplitter = new WikipediaSourceSplitter();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMapInt<String> idf;
    ByteSearch bar = ByteSearch.create("\\|");
    ByteSearch block = ByteSearch.create("[\\[\\]]");

    @Override
    public void setup(Context context) throws IOException {
        idf = new HashMapInt();
    }

    @Override
    public void map(LongWritable key, Content value, Context context) {
        Result content = wpsplitter.tokenize(value.content);
        HashSet<String> terms = new HashSet();
        //log.info("%s %d %d", value.get("collectionid"), content.text.size(), content.macro.size());
        for (byte[] text : content.text) {
            ArrayList<String> tokenize = tokenizer.tokenize(text);
            terms.addAll(tokenize);
        }
        for (byte[] text : content.macro) {
            ByteSearchPosition pos = bar.findLastPos(text, 0, text.length);
            if (pos.found()) {
                for (int i = 0; i < pos.end; i++)
                    text[i] = 0;
            }
            block.replaceAll(text, 0, text.length, "");
            ArrayList<String> tokenize = tokenizer.tokenize(text);
            terms.addAll(tokenize);
        }
        idf.add("#");
        idf.addAll(terms);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Text key = new Text();
        IntWritable value = new IntWritable();
        for (Map.Entry<String, Integer> entry : idf.entrySet()) {
            key.set(entry.getKey());
            value.set(entry.getValue());
            context.write(key, value);
        }
    }
}
