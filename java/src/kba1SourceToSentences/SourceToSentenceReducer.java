package kba1SourceToSentences;

import Sentence.SentenceWritable;
import io.github.repir.tools.hadoop.io.IntLongIntWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Assigns a unique sequence ID to sentences and writes SentenceWritables to file.
 * @author jeroen
 */
public class SourceToSentenceReducer extends Reducer<IntLongIntWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SourceToSentenceReducer.class);
    int sequence = 0;
    
    @Override
    public void reduce(IntLongIntWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            value.setID(value.getDay(), sequence++);
            context.write(NullWritable.get(), value);
        }
    }
}
