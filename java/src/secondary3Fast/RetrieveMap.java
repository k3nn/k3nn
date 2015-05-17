package secondary3Fast;

import KNN.Node;
import KNN.NodeD;
import MatchingClusterNode.MatchingClusterNodeWritable;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import stream5Retrieve.RetrieveTop3;
import kbaeval.TrecWritable;
import org.apache.hadoop.mapreduce.Mapper;
import secondary3Fast.RetrieveJob.Setting;
import static stream5Retrieve.RetrieveTop3.getNN;
import static stream5Retrieve.RetrieveTop3.getScores;

/**
 *
 * @author jeroen
 */
public class RetrieveMap extends Mapper<Setting, ClusterWritable, Setting, MatchingClusterNodeWritable> {

    public static final Log log = new Log(RetrieveMap.class);
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    Context context;
    MatchingClusterNodeWritable record = new MatchingClusterNodeWritable();
    Setting key;
    Retriever<NodeD> retriever = null;

    @Override
    public void setup(Context context) throws IOException {
        this.context = context;
    }

    @Override
    public void map(Setting key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        if (retriever == null) {
            this.key = key;
            retriever = new Retriever(key);
        }
        retriever.stream(createCluster(value));
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
    public void cleanup(Context context) {
        retriever = null;
    }

    class Retriever<N extends Node> extends RetrieveTop3<N> {

        Retriever(Setting setting) throws IOException {
            this.windowRelevanceModelHours = setting.hours;
            this.maxSentenceLengthWords = setting.length;
            this.minInformationGain = setting.gainratio;
            this.minRankObtained = setting.topk;
            this.init(setting.topicid, setting.topicstart, setting.topicend, setting.query);
            log.info("topic %d %d %d %s", setting.topicid, setting.topicstart, setting.topicend, setting.query);
            log.info("settings %f %f %d %d", setting.gainratio, setting.hours, setting.length, setting.topk);
        }

        @Override
        public void emit(int topic, Node u, String title) throws IOException, InterruptedException {
            record.clusterID = topic;
            record.creationTime = u.getCreationTime();
            record.domain = u.getDomain();
            record.nnid = u.getNN();
            record.nnscore = u.getScore();
            record.content = title;
            record.sentenceID = u.getID();
            record.documentID = ((NodeD) u).getDocumentID();
            record.sentenceNumber = ((NodeD) u).sentence;
            log.info("out %f %f %d %d %d %d %s", key.gainratio, key.hours, key.length, key.topk, key.topicid,
                    record.creationTime, record.documentID);
            context.write(key, record);
        }
    }
}
