package kba3SentencesToTitles;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import Sentence.SentenceWritable;
import io.github.htools.lib.Log;
import io.github.htools.type.Tuple2;
import io.github.htools.hadoop.LogFile;
import java.io.IOException;
import java.util.HashSet;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import Sentence.SentenceFile;

/**
 * Filter out all but the title extracted from the HTML source (i.e. row=-1)
 * @author jeroen
 */
public class SentenceToTitlesMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(SentenceToTitlesMap.class);
    NewsDomains domain = NewsDomains.instance;
    SentenceFile sf;
    Datafile df;
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
        sf = new SentenceFile(df);
        sf.openWrite();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        try {
            // use only sentenceNumber -1, which is the title extracted from the
            // HTML tags in the source document. 
            if (value.sentenceNumber == -1) {
                Tuple2<String, Integer> k = new Tuple2(value.content, value.domain);
                if (creationtime == value.creationtime) {
                    
                    // possibly a bit redundant, filters out duplicate titles 
                    // that have the same timestamp and same domain
                    if (map.contains(k))
                        return;
                } else {
                    creationtime = value.creationtime;
                }
                String dom = domain.getHost(value.domain);
                
                // possibly a bit redundant, if RemoveDuplicates was used 
                // strips non-content from titles
                value.content = TitleFilter.filterHost(dom, value.content);
                map.add(k);
                value.sentenceNumber = 0;
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
