package secondary2clusters;

import Cluster.ClusterWritable;
import KNN.Cluster;
import KNN.Stream;
import KNN.Url;
import KNN.UrlClusterListener;
import KNN.UrlD;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.LongLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterReducer extends Reducer<LongLongWritable, SentenceWritable, NullWritable, NullWritable> implements UrlClusterListener {

    public static final Log log = new Log(ClusterReducer.class);
    Configuration conf;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMap<Integer, ArrayList<ClusterWritable>> map = new HashMap();
    StreamClusterFile cf;
    long emittime;
    Stream<UrlD> stream = new Stream();

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        Datafile df = new Datafile(conf, conf.get("output"));
        cf = new StreamClusterFile(df);
        cf.openWrite();

        stream.setListenAll(this);
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
        ArrayList<String> features = tokenizer.tokenize(value.sentence);
        if (features.size() > 1) {
            HashSet<String> uniq = new HashSet(features);
            UrlD url = new UrlD(value.id, value.domain, value.sentence, uniq, value.creationtime, value.getUUID(), value.row);
            stream.urls.put(url.getID(), url);
            stream.add(url, uniq);
        }
    }

    @Override
    public void urlChanged(Url addedurl, HashSet<Url> urls) {
        if (addedurl.getCreationTime() == emittime) {
            Cluster<UrlD> cluster = addedurl.getCluster();
            if (cluster != null) {
                StreamClusterWritable cw = new StreamClusterWritable();
                cw.clusterid = cluster.getID();
                for (Url url : cluster.getUrls()) {
                    if (url != addedurl) {
                        cw.urls.add(toUrlWritable((UrlD) url));
                    }
                }
                cw.urls.add(toUrlWritable((UrlD) addedurl));
                cw.write(cf);
            }
        }
    }

    public UrlWritable toUrlWritable(UrlD url) {
        UrlWritable u = new UrlWritable();
        u.creationtime = url.getCreationTime();
        u.docid = url.getDocumentID();
        u.domain = url.getDomain();
        u.nnid = url.getNN();
        u.nnscore = url.getScore();
        u.row = url.sentence;
        u.title = url.getTitle();
        u.urlid = url.getID();
        return u;
    }
}
