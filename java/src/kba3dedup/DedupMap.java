package kba3dedup;

import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.hadoop.IO.IntLongWritable;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class DedupMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(DedupMap.class);
    IntLongWritable outkey = new IntLongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        int hashKey = MathTools.hashCode(value.domain, value.sentence.hashCode());
        outkey.set(hashKey, value.creationtime);
        context.write(outkey, value);
    }

}
