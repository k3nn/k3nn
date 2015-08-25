package secondary1docs;

import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.hadoop.ContextTools;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
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
public class RelevantDocsMap extends Mapper<LongWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RelevantDocsMap.class);
    HashMap<Integer, ArrayList<ClusterWritable>> inmap = new HashMap();
    HashMap<String, Long> out = new HashMap();
    
    @Override
    public void map(LongWritable key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<ClusterWritable> list = inmap.get(value.clusterid);
        if (list == null) {
            list = new ArrayList();
            inmap.put(value.clusterid, list);
        }
        list.add(value);
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
        for (ArrayList<ClusterWritable> list : inmap.values()) {
            Collections.sort(list, Sorter.instance);
            for (ClusterWritable current : list) {
               Collections.sort(current.nodes, SorterUrl.instance);
               long emittime = current.getCandidateNode().creationtime;
               for (NodeWritable url : current.nodes) {
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

    public void out(NodeWritable url, long emittime) {
        Long currenttime = out.get(url.docid);
        if (currenttime == null || currenttime > emittime)
            out.put(url.docid, emittime);
    }
    
    static class Sorter implements Comparator<ClusterWritable> {
        static Sorter instance = new Sorter();

        @Override
        public int compare(ClusterWritable o1, ClusterWritable o2) {
           int comp = (int)(o1.getCandidateNode().creationtime - o2.getCandidateNode().creationtime);
           if (comp == 0) {
               comp = o1.nodes.size() - o2.nodes.size();
           }
           return comp;
        }
    }
    
    static class SorterUrl implements Comparator<NodeWritable> {
        static SorterUrl instance = new SorterUrl();

        @Override
        public int compare(NodeWritable o1, NodeWritable o2) {
           return (int)(o1.creationtime - o2.creationtime);
        }
    }
}
