package stream2AddDocID;

import MatchingClusterNode.MatchingClusterNodeWritable;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.ContextTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import Sentence.SentenceWritable;

/**
 * Add the collection's documentID and sentenceNumber of nodes in query matching
 * clusters.
 * @author jeroen
 */
public class ClusterDocidReducer extends Reducer<IntWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterDocidReducer.class);
    Conf conf;
    Datafile infile;
    ArrayList<ClusterWritable> retrieved;
    HashMap<Long, ArrayList<NodeWritable>> nodesPerSentenceID;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        ArrayList<Datafile> inFiles = ClusterDocidJob.getResultFiles(conf);
        infile = inFiles.get(ContextTools.getTaskID(context));
        readRetrieved(infile);
    }

    @Override
    public void reduce(IntWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            ArrayList<NodeWritable> records = nodesPerSentenceID.get(value.sentenceID);
            for (NodeWritable record : records) {
               record.docid = value.getDocumentID();
               record.sentenceNumber = value.sentenceNumber;
            }
        }
    }
    
    private class Sorter implements Comparator<NodeWritable> {

        @Override
        public int compare(NodeWritable o1, NodeWritable o2) {
            return (int)(o1.creationtime - o2.creationtime);
        }
        
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Sorter sorter = new Sorter();
        ArrayMap<Long, ClusterWritable> sorted = new ArrayMap();
        for (ClusterWritable cluster : retrieved) {
            Collections.sort(cluster.nodes, sorter);
            NodeWritable lastUrl = cluster.nodes.get(cluster.nodes.size()-1);
            sorted.add(lastUrl.creationtime, cluster);
        }
        
        ClusterFile cf = getOutFile();
        cf.openWrite();
        for (ClusterWritable w : sorted.ascending().values())
            w.write(cf);
        cf.closeWrite();
    }

    public ClusterFile getOutFile() {
        HDFSPath dir = new HDFSPath(conf, conf.get("output"));
        Datafile outfile = dir.getFile(infile.getName());
        return new ClusterFile(outfile);
    }
    
    public void readRetrieved(Datafile df) {
        retrieved = new ArrayList();
        nodesPerSentenceID = new HashMap();
        df.setBufferSize(100000000);
        ClusterFile cf = new ClusterFile(df);
        for (ClusterWritable w : cf) {
            retrieved.add(w);
            for (NodeWritable u : w.nodes) {
                ArrayList<NodeWritable> list = nodesPerSentenceID.get(u.sentenceID);
                if (list == null) {
                    list = new ArrayList();
                    nodesPerSentenceID.put(u.sentenceID, list);
                }
                list.add(u);
            }
        }
    }
    
    public int getTopicID(Conf conf, int topic) {
        TopicFile tf = new TopicFile(new Datafile(conf, conf.get("topicfile")));
        for (TopicWritable t : tf) {
            if (topic-- == 0) {
                tf.closeRead();
                return t.id;
            }
        }
        return -1;
    }
}
