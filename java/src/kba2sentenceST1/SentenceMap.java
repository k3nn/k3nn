package kba2sentenceST1;

import kba2sentence0.*;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import java.util.HashSet;
import kba1raw.Domain_KBA;
import kba1raw.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import streamcorpus.sentence.SentenceFile;

/**
 *
 * @author jeroen
 */
public class SentenceMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(SentenceMap.class);
    Domain_KBA domain = new Domain_KBA();
    SentenceFile sf;
    Datafile df;
    LogFile logfile;
    HashSet<Tuple2<String, Integer>> map = new HashSet();
    long creationtime = 0;

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSplit fs = (FileSplit) context.getInputSplit();
        String inpath = fs.getPath().toString();
        String date = inpath.substring(inpath.lastIndexOf('/') + 1);
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        df = outdir.getFile(date);
        log.info("setup %s %b %d", df.getCanonicalPath(), df.exists(), df.exists() ? df.getLength() : -1);
        logfile = new LogFile(df);
        logfile.write("%d %s", System.currentTimeMillis() / 1000, df.getCanonicalPath());
        sf = new SentenceFile(df);
        sf.openWrite();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        try {
            if (value.row == -1) {
                Tuple2<String, Integer> k = new Tuple2(value.sentence, value.domain);
                if (creationtime == value.creationtime) {
                    if (map.contains(k))
                        return;
                } else {
                    creationtime = value.creationtime;
                }
                String dom = domain.getHost(value.domain);
                value.sentence = TitleFilter.filterHost(dom, value.sentence);
                map.add(k);
                value.row = 0;
                value.write(sf);
            }
        } catch (Exception ex) {
            log.fatal("Exception %s %s", ex.getMessage(), df.getCanonicalPath());
        }
    }

    @Override
    public void cleanup(Context context) {
        sf.closeWrite();
    }
}
