package CreateIDF;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write the cluster snapshots to a file per topic.
 * @author jeroen
 */
public class CreateIDFReducer extends Reducer<Text, IntWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(CreateIDFReducer.class);
    Datafile df;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        df = new Datafile(conf, conf.get("output"));
        df.openWrite();
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int docfreq = 0;
        for (IntWritable value : values) {
            docfreq += value.get();
        }
        if (docfreq > 9)
           df.printf("%s\t%d\n", key.toString(), docfreq);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        df.closeWrite();
    }
}
