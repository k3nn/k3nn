package stream5Retrieve;

import KNN.Node;
import KNN.NodeD;
import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import io.github.htools.extract.modules.RemoveStopwordsInquery;
import io.github.htools.io.Datafile;
import io.github.htools.extract.modules.RemoveStopwordsSmart;
import io.github.htools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import kbaeval.TopicWritable;

public class RetrieveTop3File extends RetrieveTop3<NodeD> {

    private static final Log log = new Log(RetrieveTop3File.class);
    TrecFile outfile;
    TrecWritable record = new TrecWritable();
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();
    MatchingClusterNodeFile outclusterfile;

    public RetrieveTop3File(Datafile out) {
        outfile = new TrecFile(out);
        outfile.openWrite();
        outclusterfile = new MatchingClusterNodeFile(out.getDir().getFile(out.getName() + ".titles"));
        outclusterfile.openWrite();
    }

    public void process(TopicWritable topic, Datafile in) throws IOException, InterruptedException {
        init(topic.id, topic.start, topic.end, Query.create(tokenizer, topic.query));
        ClusterFile cf = new ClusterFile(in);
        cf.setBufferSize(100000000);
        for (ClusterWritable cw : cf) {
            stream(createCluster(cw));
        }
    }

    /**
     * @param clusterWritable
     * @return reconstructed nearest neighbor cluster from stored record
     */
    public Cluster<NodeD> createCluster(ClusterWritable clusterWritable) {
        Stream<NodeD> s = new Stream();
        for (NodeWritable r : clusterWritable.nodes) {
            HashSet<String> title = new HashSet(tokenizer.tokenize(r.content));
            NodeD u = new NodeD(r.sentenceID, r.domain, r.content, title, r.creationtime, r.getUUID(), r.sentenceNumber);
            s.nodes.put(u.getID(), u);
        }
        Cluster<NodeD> c = s.createCluster(clusterWritable.clusterid);
        for (NodeWritable r : clusterWritable.nodes) {
            Node url = s.nodes.get(r.sentenceID);
            ArrayList<Long> nn = getNN(r.nnid);
            ArrayList<Double> score = getScores(r.nnscore);
            for (int i = 0; i < nn.size(); i++) {
                Node u = s.nodes.get(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                url.add(e);
            }
            url.setCluster(c);
        }
        c.setBase(Cluster.getBase(c.getNodes()));
        return c;
    }    
    
    @Override
    public void emit(int topic, NodeD u, String title) {
        record.document = u.getDocumentID();
        record.timestamp = u.getCreationTime();
        record.topic = topic;
        record.sentence = u.sentence;
        record.write(outfile);
        recordcluster.clusterID = topic;
        recordcluster.creationTime = u.getCreationTime();
        recordcluster.domain = u.getDomain();
        recordcluster.nnid = u.getNN();
        recordcluster.nnscore = u.getScore();
        recordcluster.content = title;
        recordcluster.sentenceID = u.getID();
        recordcluster.documentID = u.getDocumentID();
        recordcluster.sentenceNumber = u.sentence;
        recordcluster.write(outclusterfile);
    }

    public void close() {
        outfile.closeWrite();
        outclusterfile.closeWrite();
    }
}
