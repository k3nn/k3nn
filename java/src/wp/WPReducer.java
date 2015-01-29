package wp;

import io.github.repir.tools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author jeroen
 */
public class WPReducer extends Reducer<LinkWritable, IntWritable, NullWritable, LinkWritable> {

    public static final Log log = new Log(WPReducer.class);
    
    @Override
    public void reduce(LinkWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        LinkWritable outvalue = key;
        for (IntWritable value : values) {
            count += value.get();
        }
        outvalue.frequency = count;
        context.write(NullWritable.get(), outvalue);
    }
}
