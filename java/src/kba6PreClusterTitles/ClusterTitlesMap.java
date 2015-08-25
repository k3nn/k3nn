package kba6PreClusterTitles;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import KNN.Cluster;
import KNN.Edge;
import KNN.Node;
import KNN.Stream;
import KNN.NodeM;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.DateTools;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.type.Tuple2;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import Sentence.SentenceFile;
import Sentence.SentenceWritable;

/**
 * Clusters the titles that arrive in a day, expanding the clustering results of
 * the previous day. The titles arrive in order of timestamp. It assumes that
 * the input filename equals todays date in yyyy-mm-dd format. Based on this,
 * the output path is scanned to read the results at the end of yesterday's
 * clustering. the output consists of all title nodes, clustered and not
 * clustered, that have not expired the T=3 days window, i.e. unclustered nodes
 * that are less than 4 days old, and all nodes in a cluster that has at least
 * one node that has not expired. Since the similarity between nodes is 0 when
 * their creation times are more than T apart, pruning these nodes will affect
 * future clustering results, which we verified to be correct for the KBA
 * corpus.
 *
 * @author jeroen
 */
public class ClusterTitlesMap extends Mapper<LongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterTitlesMap.class);
    // tokenizes words on non-alphanumeric characters, lowercases, remove stop
    // words, but does not stem.
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    ClusterNodeWritable clusterwritable = new ClusterNodeWritable();
    Configuration conf;
    // the clustering stream
    Stream<NodeM> stream;
    Date today;
    long expireTime;

    public enum LABEL {

        CORRECT, TOOSHORT
    };

    @Override
    public void setup(Context context) throws IOException {
        Profiler.setTraceOn();
        stream = new Stream();
        conf = context.getConfiguration();
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
        String todayString = filename.substring(filename.lastIndexOf('/') + 1);
        try {
            today = DateTools.FORMAT.Y_M_D.toDate(todayString);
            expireTime = DateTools.daysBefore(today, 3).getTime() / 1000;
        } catch (ParseException ex) {
            log.fatalexception(ex, "illegal date %s", todayString);
        }
        readClusters();
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) {
        ArrayList<String> features = tokenizer.tokenize(value.content);
        try {
            // only use titles with more than 1 word
            if (features.size() > 1) {
                // use unique non stop words in title
                HashSet<String> uniq = new HashSet(features);
                context.getCounter(LABEL.CORRECT).increment(1);

                // create node and add to cluster graph
                NodeM node = new NodeM(value.sentenceID, value.domain, value.creationtime, uniq);
                stream.nodes.put(node.getID(), node);
                stream.add(node, uniq);
            } else {
                context.getCounter(LABEL.TOOSHORT).increment(1);
            }
        } catch (Exception ex) {
            log.fatalexception(ex, "");
        }
    }

    /**
     * write the non expired clusters and unclustered nodes at the end of the
     * day
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // map<nodeID, Node> of all non expired nodes
        HashMap<Long, Node> out = new HashMap();
        for (Cluster<NodeM> cluster : stream.getClusters()) {
            outCluster(out, cluster);
        }
        addUnclustered(out);
        stream = null; // free memory

        // map<clusterID, Node> that is sorted
        ArrayMap<Integer, Node> clusterNodes = new ArrayMap();
        for (Node url : out.values()) {
            Cluster c = url.getCluster();
            clusterNodes.add(c == null ? -1 : c.getID(), url);
        }
        out = null; // free memory

        // fetch titles from file and add to nodes
        HashMap<Long, String> titles = this.getTitlesYesterday();
        titles.putAll(this.getTitlesToday());

        // write to file
        ClusterNodeFile cf = new ClusterNodeFile(new Datafile(conf, conf.get("output")));
        cf.openWrite();
        for (Node url : clusterNodes.ascending().values()) {
            clusterwritable.set(url);
            clusterwritable.content = titles.get(url.getID());
            clusterwritable.write(cf);
        }
        cf.closeWrite();
    }

    // add all node from non-expired clusters, i.e. containing a node that has not expired
    private void outCluster(HashMap<Long, Node> out, Cluster<NodeM> cluster) throws IOException, InterruptedException {
        for (Node checkurl : cluster.getNodes()) {
            if (clusterNotExpired(cluster)) {
                addRec(out, new HashSet(cluster.getNodes()));
                break;
            }
        }
    }

    // add all non-expired unclustered nodes
    private void addUnclustered(HashMap<Long, Node> out) {
        HashSet<Node> urls = new HashSet();
        for (NodeM url : stream.nodes.values()) {
            if (!url.isClustered() && nodeNotExpired(url) && !out.containsKey(url.getID())) {
                urls.add(url);
            }
        }
        addRec(out, urls);
    }

    /**
     * @param cluster
     * @return true if one node was created after expireTime
     */
    public boolean clusterNotExpired(Cluster<NodeM> cluster) {
        for (Node checkurl : cluster.getNodes()) {
            if (checkurl.getCreationTime() > expireTime) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param node
     * @return true if created after expireTime
     */
    public boolean nodeNotExpired(Node node) {
        return node.getCreationTime() > expireTime;
    }

    // seems to be obselete
    public void addRec(HashMap<Long, Node> map, HashSet<Node> toadd) {
        while (toadd.size() > 0) {
            for (Node u : toadd) {
                map.put(u.getID(), u);
            }
            HashSet<Node> newadd = new HashSet();
            for (Node u : toadd) {
                if (u.getCreationTime() > this.expireTime || u.isClustered()) {
                    for (int e = 0; e < u.getEdges(); e++) {
                        Node nn = u.getNN(e).getNode();
                        if (nn != null && !map.containsKey(nn.getID())) {
                            newadd.add(nn);
                        }
                    }
                }
            }
            toadd = newadd;
        }
    }

    /**
     * Read the clustering at the end of yesterday.
     */
    public void readClusters() throws IOException {
        HDFSPath dir = new HDFSPath(conf, conf.get("output")).getParentPath();
        Date previousdate = DateTools.daysBefore(today, 1);
        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(previousdate));
        if (df.exists() && df.getLength() > 0) {
            ArrayMap3<Long, ArrayList<Long>, ArrayList<Double>> nn = new ArrayMap3();
            ClusterNodeFile cf = new ClusterNodeFile(df);
            for (ClusterNodeWritable cw : cf) {
                NodeM url = (NodeM) stream.nodes.get(cw.sentenceID);
                if (url == null) {
                    url = getNode(cw.sentenceID, cw.domain, cw.creationTime, cw.content);
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

    /**
     * @return the last clusterID found in previous cluster files
     */
    public static int findNextClusterID(HDFSPath dir, Date today) throws IOException {
        ArrayList<Datafile> filenames = dir.getFiles();
        String name = null;
        for (Datafile d : filenames) {
            if (d.getLength() > 0) {
                try {
                    if (DateTools.FORMAT.Y_M_D.toDate(d.getName()).before(today)) {
                        name = d.getName();
                    }
                } catch (ParseException ex) {
                    log.exception(ex, "findNextCluster invalid date %s", d);
                }
            }
        }
        int clusterid = -1;
        if (name != null) {
            Datafile df = dir.getFile(name);
            ClusterNodeFile cf = new ClusterNodeFile(df);
            for (ClusterNodeWritable u : cf) {
                clusterid = Math.max(clusterid, u.clusterID);
            }
        }
        return clusterid + 1;
    }

    /**
     * @return map with all titles in the cluster file of yesterday
     */
    public HashMap<Long, String> getTitlesYesterday() {
        HashMap<Long, String> result = new HashMap();
        HDFSPath dir = new HDFSPath(conf, conf.get("output")).getParentPath();
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

    /**
     * @return map with titles in the sentence file of today
     */
    public HashMap<Long, String> getTitlesToday() {
        HashMap<Long, String> result = new HashMap();
        HDFSPath dir = new HDFSPath(conf, conf.get("input")).getParentPath();

        Datafile df = dir.getFile(DateTools.FORMAT.Y_M_D.format(today));
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
     * @return created node, that was added to the stream pool and inverted index
     */
    public NodeM getNode(long sentenceid, int domain, long creationtime, String title) {
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
