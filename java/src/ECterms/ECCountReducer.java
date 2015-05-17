package ECterms;

import EC.*;
import kba1SourceToSentences.*;
import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.hadoop.io.IntLongIntWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Assigns a unique sequence ID to sentences and writes SentenceWritables to file.
 * @author jeroen
 */
public class ECCountReducer extends Reducer<IntTextInt, IntWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(ECCountReducer.class);
    HashMapInt<IntTextInt> map = new HashMapInt();
    
    @Override
    public void reduce(IntTextInt key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            map.add(key, value.get());
        }
    }
    
    @Override
    public void cleanup(Context context) {
        
    }
}
