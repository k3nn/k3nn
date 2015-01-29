package secondary1docs;

import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class RelevantDocsMap extends Mapper<LongWritable, StreamClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RelevantDocsMap.class);
    HashMap<Integer, ArrayList<StreamClusterWritable>> inmap = new HashMap();
    HashMap<String, Long> out = new HashMap();
    
    @Override
    public void map(LongWritable key, StreamClusterWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<StreamClusterWritable> list = inmap.get(value.clusterid);
        if (list == null) {
            list = new ArrayList();
            inmap.put(value.clusterid, list);
        }
        list.add(value);
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
        for (ArrayList<StreamClusterWritable> list : inmap.values()) {
            Collections.sort(list, Sorter.instance);
            for (StreamClusterWritable current : list) {
               Collections.sort(current.urls, SorterUrl.instance);
               long emittime = current.lastUrl().creationtime;
               for (UrlWritable url : current.urls) {
                  out(url, emittime);
               }
            }
        }
        DocFile df = outputFile(context);
        df.openWrite();
        DocWritable record = new DocWritable();
        ArrayMap<Long, String> sorted = ArrayMap.invert(out.entrySet()).ascending();
        for (Map.Entry<Long, String> entry : sorted) {
           record.docid = entry.getValue();
           record.emit = entry.getKey();
           record.write(df);
        }
        df.closeWrite();
    }
    
    public DocFile outputFile(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String filename = ContextTools.getInputPath(context).getName();
        HDFSPath outdir = new HDFSPath(conf, conf.get("output"));
        return new DocFile(outdir.getFile(filename));
    }

    public void out(UrlWritable url, long emittime) {
        Long currenttime = out.get(url.docid);
        if (currenttime == null || currenttime > emittime)
            out.put(url.docid, emittime);
    }
    
    static class Sorter implements Comparator<StreamClusterWritable> {
        static Sorter instance = new Sorter();

        @Override
        public int compare(StreamClusterWritable o1, StreamClusterWritable o2) {
           int comp = (int)(o1.lastUrl().creationtime - o2.lastUrl().creationtime);
           if (comp == 0) {
               comp = o1.urls.size() - o2.urls.size();
           }
           return comp;
        }
    }
    
    static class SorterUrl implements Comparator<UrlWritable> {
        static SorterUrl instance = new SorterUrl();

        @Override
        public int compare(UrlWritable o1, UrlWritable o2) {
           return (int)(o1.creationtime - o2.creationtime);
        }
    }
}
