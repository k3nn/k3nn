package scrape1.job;

import io.github.repir.tools.Lib.DateTools;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.StrTools;
import io.github.repir.tools.hadoop.IO.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.LogMessageFile;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scrape.Scrape;
import static scrape.Scrape.log;
import scrape.ScrapeMain;
import scrape.date.SnapshotWritable;

/**
 *
 * @author jeroen
 */
public class SnapshotReducer extends Reducer<IntWritable, Text, NullWritable, SnapshotWritable> {

    public static final Log log = new Log(SnapshotReducer.class);
    LogMessageFile logfile;
    SnapshotWritable outvalue = new SnapshotWritable();
    Domain_IA domain = Domain_IA.instance;
    Date startdate;
    Date enddate;

    @Override
    public void setup(Context context) throws IOException {
        logfile = new LogMessageFile(context);
        setDates(context);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String domainexpression = value.toString();
            SnapshotWritable outvalue = new SnapshotWritable();
            outvalue.domain = domain.getHost(key.get());
            Scrape scrape = new Scrape("https://web.archive.org/web/*/" + outvalue.domain);
            log.info("domain %s %d", outvalue.domain, scrape.times.size());
            for (String year : new String[]{"2011", "2012", "2013"}) {
                String yearurl = scrape.getYearUrl(year);
                if (yearurl != null) {
                Scrape y = new Scrape("https://web.archive.org" + yearurl);
                for (Map.Entry<String, String> entry : y.times.entrySet()) {
                    String time = entry.getKey();
                    if (validDate(time)) {
                        outvalue.url = entry.getValue();
                        outvalue.creationtime = time;
                        context.write(NullWritable.get(), outvalue);
                    }
                }
                }
            }
        }
        //logfile.write("%s %d", outvalue.domain, keys);
    }

    private boolean validDate(String time) {
        if (!time.endsWith("*")) {
            try {
                Date date = DateTools.FORMAT.YMD.parse(time.substring(0, 8));
                if (date.equals(startdate)
                        || date.equals(enddate)
                        || (date.after(startdate) && date.before(enddate))) {
                    return true;
                }
            } catch (ParseException ex) {
                logfile.write("invalid date %s", time);
            }
        }
        return false;
    }

    private void setDates(Context context) {
        Configuration conf = context.getConfiguration();
        String startdate = conf.get("startdate");
        String enddate = conf.get("enddate");
        try {
            this.startdate = DateTools.FORMAT.Y_M_D.parse(startdate);
            this.enddate = DateTools.FORMAT.Y_M_D.parse(enddate);
        } catch (ParseException ex) {
            logfile.write("invalid date %s %s", startdate, enddate);
        }
    }
}
