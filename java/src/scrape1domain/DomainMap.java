package scrape1domain;

import io.github.repir.tools.lib.Log;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class DomainMap extends Mapper<NullWritable, NullWritable, IntWritable, Text> {

    public static final Log log = new Log(DomainMap.class);

    @Override
    public void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        String[] domains = Domain_IA.instance.getDomains();
        for (int i = 0; i < domains.length; i++) {
            log.info("%s", domains[i]);
            context.write(new IntWritable(i), new Text(domains[i]));
        }
    }


}
