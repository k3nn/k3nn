package stream3DocStream;

import secondary1docs.*;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.collection.HashMap3;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.KV;
import io.github.repir.tools.type.Tuple2;
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
public class DocStreamMap extends Mapper<LongWritable, ClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(DocStreamMap.class);
    HashMap<Integer, ArrayList<ClusterWritable>> inmap = new HashMap();

    enum Counters {

        candidate,
        noncandidate
    }

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
        HashMap3<String, Long, Boolean> out = new HashMap3();
        for (ArrayList<ClusterWritable> list : inmap.values()) {
            //Collections.sort(list, Sorter.instance);
            for (int i = 0; i < list.size(); i++) {
                ClusterWritable current = list.get(i);
                Collections.sort(current.nodes, SorterUrl.instance);
                long emittime = current.getCandidateNode().creationtime;
                for (NodeWritable url : current.nodes) {
                    if (!out.containsKey(url)) {
                        out.put(url.docid, url.creationtime, i == (list.size() - 1));
                        if (i == (list.size() - 1)) {
                            context.getCounter(Counters.candidate).increment(1);
                        } else {
                            context.getCounter(Counters.noncandidate).increment(1);
                        }
                    }
                }
            }
        }
        DocFile df = outputFile(context);
        df.openWrite();
        DocWritable record = new DocWritable();
        ArrayMap3<Long, Boolean, String> sorted = new ArrayMap3();
        for (Map.Entry<String, KV<Long, Boolean>> entry : out.entrySet()) {
            sorted.add(entry.getValue().key, entry.getValue().value, entry.getKey());
        }
        for (Map.Entry<Long, Tuple2<Boolean, String>> entry : sorted.ascending()) {
            record.docid = entry.getValue().value;
            record.creationtime = entry.getKey();
            record.isCandidate = entry.getValue().key;
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

    static class Sorter implements Comparator<ClusterWritable> {

        static Sorter instance = new Sorter();

        @Override
        public int compare(ClusterWritable o1, ClusterWritable o2) {
            int comp = (int) (o1.getCandidateNode().creationtime - o2.getCandidateNode().creationtime);
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
            return (int) (o1.creationtime - o2.creationtime);
        }
    }
}
