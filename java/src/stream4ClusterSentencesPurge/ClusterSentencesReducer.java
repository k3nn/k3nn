package stream4ClusterSentencesPurge;

import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Stream;
import KNN.Node;
import KNN.NodeD;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceWritable;
import io.github.htools.hadoop.io.IntBoolWritable;
import io.github.htools.hadoop.io.LongBoolWritable;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesReducer extends Reducer<LongBoolWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterSentencesReducer.class);
    Configuration conf;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMap<Integer, ArrayList<ClusterNodeWritable>> map = new HashMap();
    ClusterFile cf;
    Stream<NodeD> stream = new Stream();
    long threshold = 0;
    final long secondsPerDay = 24 * 60 * 60;
    int count = 0;

    enum Counter {

        candidate,
        noncandidate
    }

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        Datafile df = new Datafile(conf, conf.get("output"));
        cf = new ClusterFile(df);
        cf.openWrite();
    }

    @Override
    public void reduce(LongBoolWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            addSentence(value, key.getValue2());
            if (key.getValue2()) {
                context.getCounter(Counter.candidate).increment(1);
            } else {
                context.getCounter(Counter.noncandidate).increment(1);
            }
            if (value.creationtime > threshold || count++ > 1000) {
                if (threshold == 0) {
                    threshold = ((value.creationtime / secondsPerDay) + 5) * secondsPerDay;
                } else {
                    purge(value.creationtime);
                    threshold += secondsPerDay;
                }
                count = 0;
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        cf.closeWrite();
    }

    public void addSentence(SentenceWritable value, boolean isCandidate) {
        ArrayList<String> features = tokenizer.tokenize(value.content);
        if (features.size() > 1) {
            HashSet<String> uniq = new HashSet(features);
            NodeD node = new NodeD(value.sentenceID, value.domain, value.content, uniq, value.creationtime, value.getUUID(), value.sentenceNumber);
            stream.nodes.put(node.getID(), node);
            stream.add(node, uniq);
            if (node.isClustered() && isCandidate) {
                urlChanged(node);
            }
        }
    }

    public void purge(long time) {
        long purgethresh = time - 4 * secondsPerDay;
        Iterator<Cluster<NodeD>> iter = stream.clusters.values().iterator();
        LOOP:
        while (iter.hasNext()) {
            Cluster<NodeD> c = iter.next();
            for (NodeD u : c.getNodes()) {
                if (u.getCreationTime() >= purgethresh) {
                    continue LOOP;
                }
            }
            iter.remove();
            for (NodeD n : c.getNodes())
                stream.nodes.remove(n.getID());
        }
        stream.iinodes.purge(purgethresh);
        Iterator<NodeD> itern = stream.nodes.values().iterator();
        while (itern.hasNext()) {
            NodeD n = itern.next();
            if (!n.isClustered() && n.getCreationTime() < purgethresh)
                itern.remove();;
        }
    }

    public void urlChanged(Node addedurl) {
        Cluster<NodeD> cluster = addedurl.getCluster();
        if (cluster != null) {
            ClusterWritable cw = new ClusterWritable();
            cw.clusterid = cluster.getID();
            for (Node url : cluster.getNodes()) {
                if (url != addedurl) {
                    cw.nodes.add(toUrlWritable((NodeD) url));
                }
            }
            cw.nodes.add(toUrlWritable((NodeD) addedurl));
            cw.write(cf);
        }
    }

    public NodeWritable toUrlWritable(NodeD url) {
        NodeWritable u = new NodeWritable();
        u.creationtime = url.getCreationTime();
        u.docid = url.getDocumentID();
        u.domain = url.getDomain();
        u.nnid = url.getNN();
        u.nnscore = url.getScore();
        u.sentenceNumber = url.sentence;
        u.content = url.getContent();
        u.sentenceID = url.getID();
        return u;
    }
}
