package secondary2clusters;

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
import io.github.htools.hadoop.io.LongLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterReducer extends Reducer<LongLongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterReducer.class);
    Configuration conf;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMap<Integer, ArrayList<ClusterNodeWritable>> map = new HashMap();
    ClusterFile cf;
    long emittime;
    Stream<NodeD> stream = new Stream();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        Datafile df = new Datafile(conf, conf.get("output"));
        cf = new ClusterFile(df);
        cf.openWrite();

        //stream.setListenAll(this);
    }

    @Override
    public void reduce(LongLongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        emittime = key.get();
        for (SentenceWritable value : values) {
            addSentence(value);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        cf.closeWrite();
    }

    public void addSentence(SentenceWritable value) {
        ArrayList<String> features = tokenizer.tokenize(value.content);
        if (features.size() > 1) {
            HashSet<String> uniq = new HashSet(features);
            NodeD url = new NodeD(value.sentenceID, value.domain, value.content, uniq, value.creationtime, value.getUUID(), value.sentenceNumber);
            stream.nodes.put(url.getID(), url);
            stream.add(url, uniq);
            if (url.isClustered())
                urlChanged(url);
        }
    }

    public void urlChanged(Node addedurl) {
        if (addedurl.getCreationTime() == emittime) {
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
