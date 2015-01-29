package detail1docs;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class LinesMap extends Mapper<LongWritable, SentenceWritable, IntLongWritable, SentenceWritable> {

    public static final Log log = new Log(LinesMap.class);
    Conf conf;
    HashMap<Integer, ArrayList<Integer>> mapDocReducer;
    IntLongWritable outkey = new IntLongWritable();
    HashSet<UUID> docids = new HashSet();

    @Override
    public void setup(Context context) throws IOException {
        conf = Conf.convert(context.getConfiguration());
        mapDocReducer = LinesJob.mapDocReducer(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> list = mapDocReducer.get(value.id);
        if (list != null) {
            docids.add(value.getUUID());
            for (int reducer : list) {
                outkey.set(reducer, value.creationtime);
                context.write(outkey, value);
            }
        } else if (docids.contains(value.getUUID())) {
            for (int reducer : list) {
                outkey.set(reducer, value.creationtime);
                context.write(outkey, value);
            }
        }
    }
}
