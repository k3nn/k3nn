package stream1ClusterTitles;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Node;
import KNN.NodeM;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import Sentence.SentenceFile;
import Sentence.SentenceWritable;
import static kba6PreClusterTitles.ClusterTitlesMap.findNextClusterID;

/**
 * Clusters the titles of one single day, starting with the clustering results
 * at the end of yesterday,
 *
 * @author jeroen
 */
public class ClusterTitlesMap extends Mapper<LongWritable, SentenceWritable, IntWritable, ClusterWritable> {

    public static final Log log = new Log(ClusterTitlesMap.class);
    // tokenizes on non-alphanumeric characters, lowercase, stop words removed, no stemming
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    // Map<TopicID, List<SentenceID that contain all query terms>>
    HashMap<Long, ArrayList<Integer>> topicSentences;
    // Map of topicID and query matching cluster
    ArrayMap<IntWritable, ClusterWritable> out = new ArrayMap();
    Configuration conf;
    String inputfile;
    // clustering graph used to cluster the sentences
    Stream<NodeM> stream;
    Date today;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        topicSentences = ClusterTitlesJob.getTopicSentences(conf);
        stream = new Stream();
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        inputfile = ((FileSplit) context.getInputSplit()).getPath().toString();

        // extract today from input file, e.g. filename must be yyyy-mm-dd
        String todayString = filename.substring(filename.lastIndexOf('/') + 1);
        try {
            today = DateTools.FORMAT.Y_M_D.toDate(todayString);
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
        }
        readClusters();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        ArrayList<String> features = tokenizer.tokenize(value.content);
        try {
            // only use titles with more than one word
            if (features.size() > 1) {
                // clustering is done on binary vectors
                HashSet<String> uniq = new HashSet(features);

                // add title to clustering graph
                NodeM url = new NodeM(value.sentenceID, value.domain, value.creationtime, uniq);
                NodeM exists = stream.nodes.get(url.getID());
                if (exists != null) {
                    log.info("existing %d %s", url.getID(), value.creationtime, value.content);
                }
                stream.nodes.put(url.getID(), url);
                stream.add(url, uniq);

                // if clustered and it is a query matching cluster, prepare to emit
                if (url.isClustered()) {
                    this.addCandidateNode(url);
                }
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        stream = null; // clean memory

        // read titles for the clusters to emit
        HashMap<Long, String> titles = this.getTitlesYesterday();
        titles.putAll(this.getTitlesToday());

        // add titles to nodes, and emit
        for (Map.Entry<IntWritable, ClusterWritable> entry : out) {
            ClusterWritable w = entry.getValue();
            for (NodeWritable u : w.nodes) {
                u.content = titles.get(u.sentenceID);
            }
            Collections.sort(w.nodes, Sorter.instance);
            context.write(entry.getKey(), w);
        }
    }

    /**
     * if candidateNode is in a query matching cluster, prepare to emit
     *
     * @param candidateNode
     */
    public void addCandidateNode(Node candidateNode) {
        Cluster<NodeM> cluster = candidateNode.getCluster();
        HashSet<Integer> reducers = new HashSet();
        for (Node node : cluster.getNodes()) {
            Collection<Integer> topics = topicSentences.get(node.getID());
            if (topics != null) {
                reducers.addAll(topics);
            }
        }
        for (int reducer : reducers) {
            addQueryMatchingClusterToReducer(reducer, candidateNode, cluster);
        }
    }

    /**
     * Prepare a query matching cluster for emission to the reducer of the topic
     *
     * @param reducer
     * @param candidateNode
     * @param cluster
     */
    public void addQueryMatchingClusterToReducer(int reducer, Node candidateNode, Cluster<NodeM> cluster) {
        ClusterWritable w = new ClusterWritable();
        w.clusterid = cluster.getID();
        for (Node node : cluster.getNodes()) {
            if (node != candidateNode) {
                w.nodes.add(toNodeWritable(node));
            }
        }
        w.nodes.add(toNodeWritable(candidateNode));
        IntWritable key = new IntWritable(reducer);
        out.add(key, w);
    }

    public NodeWritable toNodeWritable(Node node) {
        NodeWritable n = new NodeWritable();
        n.creationtime = node.getCreationTime();
        n.domain = node.getDomain();
        n.nnid = node.getNN();
        n.nnscore = node.getScore();
        n.sentenceID = node.getID();
        return n;
    }

    // sort on creation time
    private static class Sorter implements Comparator<NodeWritable> {

        static Sorter instance = new Sorter();

        @Override
        public int compare(NodeWritable o1, NodeWritable o2) {
            return (int) (o1.creationtime - o2.creationtime);
        }
    }

    /**
     * read cluster snapshot at the end of yesterday, as the starting point to 
     * cluster todays titles in a simulated online setting
     */
    public void readClusters() throws IOException, IOException, IOException, IOException, IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("clusters"));
        Date previousdate = DateTools.daysBefore(today, 1);
        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        if (df.exists() && df.getLength() > 0) {
            ArrayMap3<Long, ArrayList<Long>, ArrayList<Double>> nn = new ArrayMap3();
            ClusterNodeFile cf = new ClusterNodeFile(df);
            for (ClusterNodeWritable cw : cf) {
                NodeM url = (NodeM) stream.nodes.get(cw.sentenceID);
                if (url == null) {
                    url = getUrl(cw.sentenceID, cw.domain, cw.creationTime, cw.content);
                    nn.add(url.getID(), cw.getNN(), cw.getNNScore());
                    if (cw.clusterID >= 0) {
                        Cluster c = stream.getCluster(cw.clusterID);
                        if (c == null) {
                            c = stream.createCluster(cw.clusterID);
                        }
                        url.setCluster(c);
                    }
                }
            }
            for (Map.Entry<Long, Tuple2<ArrayList<Long>, ArrayList<Double>>> entry : nn) {
                NodeM url = stream.nodes.get(entry.getKey());
                for (int i = 0; i < entry.getValue().key.size(); i++) {
                    long nnid = entry.getValue().key.get(i);
                    NodeM nnurl = stream.nodes.get(nnid);
                    Edge e = new Edge(nnurl, entry.getValue().value.get(i));
                    url.add(e);
                }
            }
        } else {
            stream.setStartClusterID(findNextClusterID(dir, today));
        }

        ArrayList<Cluster> clusters = new ArrayList(stream.getClusters());
        for (Cluster c : clusters) {
            c.recheckBase();
        }
        //stream.resetChangedCluster();
    }

    public HashMap<Long, String> getTitlesYesterday() {
        HashMap<Long, String> result = new HashMap();
        HDFSPath dir = new HDFSPath(conf, conf.get("clusters"));
        Date previousdate = DateTools.daysBefore(today, 1);

        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        if (df.exists()) {
            ClusterNodeFile cf = new ClusterNodeFile(df);
            for (ClusterNodeWritable cw : cf) {
                result.put(cw.sentenceID, cw.content);
            }
        }
        return result;
    }

    public HashMap<Long, String> getTitlesToday() {
        HashMap<Long, String> result = new HashMap();
        Datafile df = new Datafile(conf, inputfile);
        if (df.exists()) {
            SentenceFile cf = new SentenceFile(df);
            for (SentenceWritable cw : cf) {
                result.put(cw.sentenceID, cw.content);
            }
        }
        return result;
    }

    /**
     * @param sentenceid
     * @param domain
     * @param creationtime
     * @param title
     * @return created Node from yesterday's snapshot, added to the stream pool and inverted index
     */
    public NodeM getUrl(long sentenceid, int domain, long creationtime, String title) {
        NodeM node = (NodeM) stream.nodes.get(sentenceid);
        if (node == null) {
            ArrayList<String> features = tokenizer.tokenize(title);
            if (features.size() > 1) {
                node = new NodeM(sentenceid, domain, creationtime, features);
                stream.nodes.put(node.getID(), node);
                stream.iinodes.add(node, features);
            }
        } 
        return node;
    }
}
