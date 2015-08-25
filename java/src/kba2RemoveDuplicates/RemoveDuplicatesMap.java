package kba2RemoveDuplicates;

import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import Sentence.SentenceWritable;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import kba1SourceToSentences.NewsDomains;
import kba1SourceToSentences.TitleFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import Sentence.SentenceFile;
import io.github.htools.hadoop.io.LongLongWritable;

/**
 * Removes duplicates that have the exact same documentID in the collection,
 * within a document duplicate sentences that have the same sentence number, 
 * and strips the extracted title from non-title elements.
 * @author jeroen
 */
public class RemoveDuplicatesMap extends Mapper<LongWritable, SentenceWritable, LongLongWritable, SentenceWritable> {

    public static final Log log = new Log(RemoveDuplicatesMap.class);
    NewsDomains domain = NewsDomains.instance;
    HashMap<Integer, SentenceWritable> map = new HashMap();
    HashSet<String> set = new HashSet();
    String documentid = "";
    LongLongWritable outkey = new LongLongWritable();

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        if (!value.getDocumentID().equals(documentid)) {
            save(context);
            documentid = value.getDocumentID();
            map = new HashMap();
        }
        map.put(value.sentenceNumber, value);
    }

    public void save(Context context) throws IOException, InterruptedException {
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
                if (w.sentenceNumber == -1) {
                    String dom = domain.getHost(w.domain);
                    w.content = TitleFilter.filterHost(dom, w.content);
                }
                outkey.set(w.creationtime, w.sentenceID);
                context.write(outkey, w);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        save(context);
    }
}
