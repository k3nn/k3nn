package stream5Retrieve.NoTitle;

import static stream5Retrieve.RetrieveJob.*;
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
import org.apache.hadoop.mapreduce.Mapper;
import stream5Retrieve.RetrieveTop3;
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
    ArrayList<Cluster<NodeD>> clusters;
    String docid = "";

    @Override
    public void setup(Context context) throws IOException {
        this.context = context;
        docid = "";
    }

    @Override
    public void map(Setting key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        //log.info("%d", value.clusterid);
        if (retriever == null) {
            this.key = key;
            retriever = new Retriever(key);
        }
        Cluster<NodeD> cluster = createCluster(value);
        NodeD node = cluster.getNodes().get(cluster.size() - 1);
        if (retriever.qualifyLength(node, node.getTerms())) {
            if (!docid.equals(node.getDocumentID())) {
                if (docid.length() > 0) {
                    cleanup(context);
                }
                clusters = new ArrayList();
                docid = node.getDocumentID();
            }
            clusters.add(cluster);
        }
    }

    /**
     * @param cw
     * @return reconstructed nearest neighbor cluster from stored record
     */
    public Cluster<NodeD> createCluster(ClusterWritable cw) {
        Stream<NodeD> s = new Stream();
        for (NodeWritable r : cw.nodes) {
            HashSet<String> title = new HashSet(tokenizer.tokenize(r.content));
            NodeD u = new NodeD(r.sentenceID, r.domain, r.content, title, r.creationtime, r.getUUID(), r.sentenceNumber);
            s.nodes.put(u.getID(), u);
        }
        Cluster<NodeD> c = s.createCluster(cw.clusterid);
        for (NodeWritable r : cw.nodes) {
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
        //log.info("cluster %d base size %d cluster size %d", c.getID(), c.getBase().size(), c.getNodes().size());
        //log.info("%s", c.evalall());
        return c;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (clusters != null && clusters.size() > 0) {
            log.info("cluster %s", docid);
            for (Cluster<NodeD> c : clusters) {
                retriever.stream(c);
            }
            //retriever.stream(clusters);
        }
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
        
    public void stream(Cluster<N> c) throws IOException, InterruptedException {
        N candidateNode = (N) c.getNodes().get(c.getNodes().size() - 1);
        c = c.stripCluster();

        // tokenize sentence to list of unique non stop words
        HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
        ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

        boolean contains = c.getNodes().contains(candidateNode);
        boolean clusterqualifies = qualifies(c, queryterms);
        boolean nodeQualifies = qualifyLength(candidateNode, candidateNodeTermSet);

        if (contains && clusterqualifies && nodeQualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
            relevanceVector.addPre(c, candidateNode);
            relevanceVector.removeExpired(candidateNode.getCreationTime());
            rankEmittedSentences();
            double similarityCandidateNode = relevanceVector.cosSim(candidateNodeTerms);
            double similarityRankR = (emittedSentences.size() >= minRankObtained) ? emittedSentences.getKey(minRankObtained - 1) : 0;
            boolean rankqualifies = (emittedSentences.size() < minRankObtained || similarityCandidateNode >= similarityRankR);

            HashSet<String> titleminusquery = new HashSet(candidateNodeTerms);
            titleminusquery.removeAll(queryterms);
            double gain = knownwords.estimateInformationGain(candidateNodeTerms, c, candidateNode);
            double gainthreshold = ((titleminusquery.size() * (candidateNodeTerms.size() - 1)) * minInformationGain);
            //gainthreshold = (((candidateNodeTerms.size() - queryterms.size()) * (candidateNodeTerms.size() - 1)) * minInformationGain);
            boolean gainqualifies = gain >= gainthreshold;

            boolean oldinfoqualifies = knownwords.getOldInfo(candidateNodeTerms, queryterms) > 0;

            if (oldinfoqualifies && gainqualifies && rankqualifies && ((NodeD)candidateNode).sentence != 0) {
                emit(topicID, candidateNode, candidateNode.getContent());
                knownwords.addKownWordCombinations(candidateNodeTerms);
                emittedSentences.add(similarityCandidateNode, candidateNode);
            }
            relevanceVector.addPost(candidateNode);
        }
    }

    }
}