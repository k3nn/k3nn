package kba2sentenceDelDup;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import streamcorpus.sentence.SentenceWritable;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
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
public class DedupMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(DedupMap.class);
    Domain_KBA domain = new Domain_KBA();
    SentenceFile sf;
    Datafile df;
    HashMap<Integer, SentenceWritable> map = new HashMap();
    HashSet<String> set = new HashSet();
    String documentid = "";

    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSplit fs = (FileSplit) context.getInputSplit();
        String inpath = fs.getPath().toString();
        String date = inpath.substring(inpath.lastIndexOf('/') + 1);
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        df = outdir.getFile(date);
        sf = new SentenceFile(df);
        sf.openWrite();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        if (!value.getDocumentID().equals(documentid)) {
            save();
            documentid = value.getDocumentID();
            map = new HashMap();
        }
        map.put(value.row, value);
    }

    public void save() {
        if (map.size() > 0) {
            SentenceWritable value = map.get(-1);
            if (value != null) {
                if (set.contains(documentid)) {
                    return;
                }
                set.add(documentid);
            }
            TreeMap<Integer, SentenceWritable> sorted = new TreeMap(map);
            for (SentenceWritable w : sorted.values()) {
                if (w.row == -1) {
                    String dom = domain.getHost(w.domain);
                    w.sentence = TitleFilter.filterHost(dom, w.sentence);
                }
                w.write(sf);
            }
        }
    }

    @Override
    public void cleanup(Context context) {
        save();
        sf.closeWrite();
    }
}
