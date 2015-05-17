package kba2RemoveDuplicates;

import Sentence.SentenceFile;
import Sentence.SentenceWritable;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.DayPartitioner;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import java.io.IOException;
import kba1SourceToSentences.ReducerKeysDays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Remove titles, if the exact same title appeared on the same domain within a week.
 * the input is grouped by a hash key on domain-title, and sorted on timestamp
 * @author jeroen
 */
public class RemoveDuplicatesReducer extends Reducer<Object, SentenceWritable, NullWritable, SentenceWritable> {

    public static final Log log = new Log(RemoveDuplicatesReducer.class);
    private long oneWeekInSeconds = 7 * 24 * 60 * 60;
    Datafile df;
    SentenceFile sf;
    int sequence = 0;

    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        String date = DayPartitioner.getDate(conf, ContextTools.getTaskID(context));
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        df = outdir.getFile(date);
        sf = new SentenceFile(df);
        sf.openWrite();
    }
    
    @Override
    public void reduce(Object key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable s : values) {
            long day = ReducerKeysDays.getDay(s.creationtime);
            s.sentenceID = (day << 22) | sequence++;
            s.write(sf);
        }
     }
    
    @Override
    public void cleanup(Context context) {
        sf.closeWrite();
    }    
}
