package scrape4merge;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchSingle;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.TextLongWritable;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import scrape.date.SnapshotWritable;
import scrape1domain.Domain_IA;

/**
 *
 * @author jeroen
 */
public class MergeMap extends Mapper<LongWritable, SnapshotWritable, TextLongWritable, SnapshotWritable> {

    public static final Log log = new Log(MergeMap.class);
    LogFile logfile;
    TextLongWritable outkey = new TextLongWritable();
    Domain_IA domain = Domain_IA.instance;
    ByteSearch questionmark = new ByteSearchSingle((byte)'?');

    public void setup(Context context) {
        logfile = new LogFile(context);
    }

    @Override
    public void map(LongWritable key, SnapshotWritable value, Context context) throws IOException, InterruptedException {
        if (value.creationtime.length() > 0) {
            int domainnr = domain.getDomainForHost(value.domain);
            ByteSearch articleregex = domain.getRegex(domainnr);
            String urlpath = value.url;
            boolean valid = articleregex.exists(urlpath);
            if (valid) {
                Long creationtime = Long.parseLong(value.creationtime);
                outkey.set(stripTimeLink(value.url), creationtime);
                context.write(outkey, value);
            }
        }
    }

    public String stripTimeLink(String hyperlink) {
        int method = hyperlink.indexOf("://");
        if (method > 0) {
            return hyperlink.substring(method + 3).trim();
        }
        return hyperlink.trim();
    }
}
