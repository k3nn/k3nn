package kba5TitleSortTimestamp;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import Sentence.SentenceWritable;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.hadoop.io.DayPartitioner;
import java.io.IOException;
import kba1SourceToSentences.ReducerKeysDays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceFile;

/**
 *
 * @author jeroen
 */
public class SortReducer extends Reducer<LongWritable, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(SortReducer.class);
    Datafile df;
    SentenceFile sf;
    int sequence = 0;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String date = DayPartitioner.getDate(conf, ContextTools.getTaskID(context));
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        df = outdir.getFile(date);
        sf = new SentenceFile(df);
        sf.openWrite();
    }

    @Override
    public void reduce(LongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable s : values) {
            int day = ReducerKeysDays.getDay(s.creationtime);
            s.sentenceID = (day << 22) | sequence++;
            s.write(sf);
        }
    }

    @Override
    public void cleanup(Context context) {
        sf.closeWrite();
    }
}
