package stream4ClusterSentences;

import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Stream;
import KNN.Node;
import KNN.NodeD;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.LongLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceWritable;
import io.github.repir.tools.hadoop.io.IntBoolWritable;
import io.github.repir.tools.hadoop.io.LongBoolWritable;

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
