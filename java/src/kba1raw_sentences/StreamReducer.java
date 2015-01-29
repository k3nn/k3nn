package kba1raw_sentences;

import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class StreamReducer extends Reducer<IntLongStringIntWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(StreamReducer.class);
    int sequence = 0;
    
    @Override
    public void reduce(IntLongStringIntWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            value.id |= (sequence++);
            context.write(NullWritable.get(), value);
        }
    }
}
