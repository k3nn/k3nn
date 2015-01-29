package kba8docid;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterDocidReducer extends Reducer<IntWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterDocidReducer.class);
    Conf conf;
    Datafile infile;
    HashMap<Integer, ArrayList<RelClusterWritable>> map = new HashMap();
    HashMap<Integer, ArrayList<RelClusterWritable>> retrieved;

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        ArrayList<Datafile> inFiles = ClusterDocidJob.getResultFiles(conf);
        infile = inFiles.get(ContextTools.getTaskID(context));
        retrieved = readRetrieved(infile);
    }

    @Override
    public void reduce(IntWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            log.info("%d", value.id);
            ArrayList<RelClusterWritable> records = retrieved.get(value.id);
            for (RelClusterWritable record : records)
               record.documentid = value.getDocumentID();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        ArrayMap<Long, RelClusterWritable> sorted = new ArrayMap();
        for (ArrayList<RelClusterWritable> list : retrieved.values()) {
            for (RelClusterWritable w : list)
               sorted.add(w.creationtime, w);
        }
        
        RelClusterFile cf = getOutFile();
        cf.openWrite();
        for (RelClusterWritable w : sorted.ascending().values())
            w.write(cf);
        cf.closeWrite();
    }

    public RelClusterFile getOutFile() {
        HDFSPath dir = new HDFSPath(conf, conf.get("output"));
        Datafile outfile = dir.getFile(infile.getFilename());
        return new RelClusterFile(outfile);
    }
    
    public HashMap<Integer, ArrayList<RelClusterWritable>> readRetrieved(Datafile df) {
        HashMap<Integer, ArrayList<RelClusterWritable>> map = new HashMap();
        ClusterFile cf = new ClusterFile(df);
        for (ClusterWritable w : cf) {
            RelClusterWritable r = new RelClusterWritable();
            r.clusterid = w.clusterid;
            r.creationtime = w.creationtime;
            r.domain = w.domain;
            r.nnid = w.nnid;
            r.nnscore = w.nnscore;
            r.title = w.title;
            r.urlid = w.urlid;
            ArrayList<RelClusterWritable> list = map.get(r.urlid);
            if (list == null) {
                list = new ArrayList();
                map.put(r.urlid, list);
            }
            list.add(r);
        }
        return map;
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
