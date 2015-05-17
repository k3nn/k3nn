package kba4TitleDeduplication;

import Sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class TitleDeduplicationMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(TitleDeduplicationMap.class);
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        // construct a key that sends titles with the same domain-title to the
        // same reducer, sorted on timestamp.
        int hashKey = MathTools.hashCode(value.domain, value.content.hashCode());
        outkey.set(hashKey, value.creationtime);
        
        context.write(outkey, value);
    }

}
