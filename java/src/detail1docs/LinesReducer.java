package detail1docs;

import KNN.Cluster;
import KNN.Stream;
import KNN.Url;
import KNN.UrlD;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class LinesReducer extends Reducer<IntLongWritable, SentenceWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(LinesReducer.class);
    Conf conf;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    Datafile infile;
    Stream<UrlD> stream = new Stream();

    @Override
    public void setup(Context context) throws IOException {
        conf = ContextTools.getConfiguration(context);
        ArrayList<Datafile> inFiles = LinesJob.getResultFiles(conf);
        infile = inFiles.get(ContextTools.getTaskID(context));
    }

    @Override
    public void reduce(IntLongWritable key, Iterable<SentenceWritable> values, Context context) throws IOException, InterruptedException {
        for (SentenceWritable value : values) {
            HashSet<String> features = new HashSet(tokenizer.tokenize(value.sentence));
            UrlD url = new UrlD(value.id, value.domain, value.sentence, features, value.creationtime, value.getUUID(), value.row);
            stream.add(url, features);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        ArrayMap<Long, RelClusterWritable> sorted = new ArrayMap();
        for (Cluster<UrlD> c : stream.getClusters()) {
            for (UrlD u : c.getUrls()) {
                RelClusterWritable w = new RelClusterWritable();
                w.clusterid = c.getID();
                w.creationtime = u.getCreationTime();
                w.documentid = ((UrlD)u).getDocumentID();
                w.domain = u.getDomain();
                w.nnid = u.getNN();
                w.nnscore = u.getScore();
                w.title = u.getTitle();
                w.urlid = u.getID();
                w.row = ((UrlD)u).sentence;
                sorted.add(w.creationtime, w);
            }
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
}
