package stream4ClusterSentencesPurge.IDF;

import stream4ClusterSentencesPurge.*;
import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Stream;
import KNN.Node;
import KNN.NodeD;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import KNN.IINodes;
import KNN.IINodesIDF;
import KNN.NodeM;
import KNN.NodeMagnitude;
import KNN.Score;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMapDouble;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.IntBoolWritable;
import io.github.repir.tools.hadoop.io.LongBoolWritable;
import io.github.repir.tools.search.ByteSearch;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class ClusterSentencesReducer extends Reducer<LongBoolWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterSentencesReducer.class);
    Conf conf;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMap<Integer, ArrayList<ClusterNodeWritable>> map = new HashMap();
    ClusterFile cf;
    LocalStream stream;
    long threshold = 0;
    final long secondsPerDay = 24 * 60 * 60;
    int count = 0;

    enum Counter {

        candidate,
        noncandidate
    }

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        stream = new LocalStream(conf, conf.get("idf"));
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
            NodeMagnitude node = new NodeMagnitude(stream, value.sentenceID, value.domain, value.content, uniq, value.creationtime, value.getUUID(), value.sentenceNumber);
            stream.nodes.put(node.getID(), node);
            stream.add(node, uniq);
            if (node.isClustered() && isCandidate) {
                urlChanged(node);
            }
        }
    }

    public void purge(long time) {
        long purgethresh = time - 4 * secondsPerDay;
        Iterator<Cluster<NodeMagnitude>> iter = stream.clusters.values().iterator();
        LOOP:
        while (iter.hasNext()) {
            Cluster<NodeMagnitude> c = iter.next();
            for (NodeD u : c.getNodes()) {
                if (u.getCreationTime() >= purgethresh) {
                    continue LOOP;
                }
            }
            iter.remove();
            for (NodeD n : c.getNodes()) {
                stream.nodes.remove(n.getID());
            }
        }
        stream.iinodes.purge(purgethresh);
        Iterator<NodeMagnitude> itern = stream.nodes.values().iterator();
        while (itern.hasNext()) {
            NodeD n = itern.next();
            if (!n.isClustered() && n.getCreationTime() < purgethresh) {
                itern.remove();
            };
        }
    }

    public void urlChanged(Node addedurl) {
        Cluster<NodeMagnitude> cluster = addedurl.getCluster();
        if (cluster != null) {
            ClusterWritable cw = new ClusterWritable();
            cw.clusterid = cluster.getID();
            for (Node url : cluster.getNodes()) {
                if (url != addedurl) {
                    cw.nodes.add(toUrlWritable((NodeMagnitude) url));
                }
            }
            cw.nodes.add(toUrlWritable((NodeMagnitude) addedurl));
            cw.write(cf);
        }
    }

    public NodeWritable toUrlWritable(NodeMagnitude url) {
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

    public static class LocalStream extends Stream<NodeMagnitude> {

        public LocalStream(Conf conf, String idffile) {
            iinodes = createII(conf, idffile);
        }
        
        public IINodes createII(Conf conf, String idffile) {
            Datafile df = new Datafile(conf, idffile);
            df.setBufferSize((int)df.getLength());
            HashMapInt<String> docfreq = new HashMapInt();
            String lines[] = df.readAsString().split("\\n");
            for (String line : lines) {
                String[] part = line.split("\\s");
                docfreq.put(part[0], Integer.parseInt(part[1]));
            }
            return new IINodesIDF(docfreq.get("#"), docfreq);
        }

        @Override
        public double score(NodeMagnitude a, NodeMagnitude b, int count, double sqrta) {
            if (a.domain == b.domain) {
                return 0;
            }
            double dotproduct = 0;
            if (a.terms.size() < b.terms.size()) {
                for (String term : a.terms) {
                    if (b.terms.contains(term)) {
                        dotproduct += this.iinodes.getFreq(term);
                    }
                }
            } else {
                for (String term : b.terms) {
                    if (a.terms.contains(term)) {
                        dotproduct += this.iinodes.getFreq(term);
                    }
                }
            }
            return Score.timeliness(a.getCreationTime(), b.getCreationTime())
                    * dotproduct / (a.magnitude * b.magnitude);
        }
    }
}
