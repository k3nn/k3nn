package stream2docid;

import RelCluster.RelClusterWritable;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
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
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterDocidReducer extends Reducer<IntWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(ClusterDocidReducer.class);
    Conf conf;
    Datafile infile;
    ArrayList<StreamClusterWritable> retrieved;
    HashMap<Integer, ArrayList<UrlWritable>> urls;

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
            log.info("%d", value.id);
            ArrayList<UrlWritable> records = urls.get(value.id);
            for (UrlWritable record : records) {
               record.docid = value.getDocumentID();
               record.row = value.row;
            }
        }
    }
    
    private class Sorter implements Comparator<UrlWritable> {

        @Override
        public int compare(UrlWritable o1, UrlWritable o2) {
            return (int)(o1.creationtime - o2.creationtime);
        }
        
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Sorter sorter = new Sorter();
        ArrayMap<Long, StreamClusterWritable> sorted = new ArrayMap();
        for (StreamClusterWritable cluster : retrieved) {
            Collections.sort(cluster.urls, sorter);
            UrlWritable lastUrl = cluster.urls.get(cluster.urls.size()-1);
            sorted.add(lastUrl.creationtime, cluster);
        }
        
        StreamClusterFile cf = getOutFile();
        cf.openWrite();
        for (StreamClusterWritable w : sorted.ascending().values())
            w.write(cf);
        cf.closeWrite();
    }

    public StreamClusterFile getOutFile() {
        HDFSPath dir = new HDFSPath(conf, conf.get("output"));
        Datafile outfile = dir.getFile(infile.getFilename());
        return new StreamClusterFile(outfile);
    }
    
    public void readRetrieved(Datafile df) {
        retrieved = new ArrayList();
        urls = new HashMap();
        StreamClusterFile cf = new StreamClusterFile(df);
        for (StreamClusterWritable w : cf) {
            retrieved.add(w);
            for (UrlWritable u : w.urls) {
                ArrayList<UrlWritable> list = urls.get(u.urlid);
                if (list == null) {
                    list = new ArrayList();
                    urls.put(u.urlid, list);
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
