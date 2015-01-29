package kba2sentences_ia;

import kba3dedup.*;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class SentenceIAReducer extends Reducer<IntLongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SentenceIAReducer.class);
    int sequence = 0;
    HashMap<Integer, HashMap<String, SentenceWritable>> map = new HashMap();

    @Override
    public void reduce(IntLongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer, HashMap<String, SentenceWritable>> map = new HashMap();
        for (SentenceWritable value : values) {
            HashMap<String, SentenceWritable> list = map.get(value.domain);
            if (list == null) {
                list = new HashMap();
                map.put(value.domain, list);
            }
            SentenceWritable existing = list.get(value.sentence);
            if (existing == null) {
                value.id |= (sequence++);
                list.put(value.sentence, value);
            } else {
                log.info("%d %d %b", value.creationtime, existing.creationtime, 
                        value.creationtime - existing.creationtime > 7 * 24 * 60 * 60);
                if (value.creationtime - existing.creationtime > 7 * 24 * 60 * 60) {
                    context.write(NullWritable.get(), existing);
                    value.id |= (sequence++);
                    list.put(value.sentence, value);
                }
            }
        }
        for (HashMap<String, SentenceWritable> entry : map.values()) {
            for (SentenceWritable w : entry.values()) {
                context.write(NullWritable.get(), w);
            }
        }
    }
}