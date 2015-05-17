package kba5TitleSortTimestamp;

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
public class SortMap extends Mapper<LongWritable, SentenceWritable, LongWritable, SentenceWritable> {

    public static final Log log = new Log(SortMap.class);
    LongWritable outkey = new LongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        outkey.set(value.creationtime);
        context.write(outkey, value);
    }

}
