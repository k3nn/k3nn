package kba4TitleDeduplication;

import Sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Remove titles, if the exact same title appeared on the same domain within a week.
 * the input is grouped by a hash key on domain-title, and sorted on timestamp
 * @author jeroen
 */
public class TitleDeduplicationReducer extends Reducer<IntLongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(TitleDeduplicationReducer.class);
    private long oneWeekInSeconds = 7 * 24 * 60 * 60;

    @Override
    public void reduce(IntLongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer, HashMap<String, SentenceWritable>> map = new HashMap();
        for (SentenceWritable value : values) {
            HashMap<String, SentenceWritable> list = map.get(value.domain);
            if (list == null) {
                list = new HashMap();
                map.put(value.domain, list);
            }
            SentenceWritable existing = list.get(value.content);
            if (existing == null) {
                list.put(value.content, value);
            } else {
                // don't write if the same title appeared in the same domain within a week
                if (value.creationtime - existing.creationtime > oneWeekInSeconds) {
                    context.write(NullWritable.get(), existing);
                    list.put(value.content, value);
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
