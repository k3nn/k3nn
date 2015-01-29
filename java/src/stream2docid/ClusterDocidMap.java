package stream2docid;

import StreamCluster.StreamClusterWritable;
import kba8docid.*;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterDocidMap extends Mapper<LongWritable, SentenceWritable, IntWritable, SentenceWritable> {

    public static final Log log = new Log(ClusterDocidMap.class);
    Conf conf;
    HashMap<Integer, ArrayList<Integer>> articles;
    IntWritable outkey = new IntWritable();

    @Override
    public void setup(Context context) throws IOException {
        conf = Conf.convert(context.getConfiguration());
        articles = ClusterDocidJob.getArticles(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> list = articles.get(value.id);
        if (list != null) {
            for (int reducer : list) {
                outkey.set(reducer);
                context.write(outkey, value);
            }
        }
    }
}
