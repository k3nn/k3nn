package secondary3TopTune;

import KNN.UrlD;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.io.HDFSPath;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import secondary3TopTune.RetrieveJob.Setting;

/**
 *
 * @author jeroen
 */
public class RetrieveReducer extends Reducer<Setting, RelClusterWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RetrieveReducer.class);
    Configuration conf;
    HDFSPath outpath;
    TrecFile trecfile;
    TrecWritable record = new TrecWritable();
    RelClusterWritable recordcluster = new RelClusterWritable();
    RelClusterFile outclusterfile;
    ArrayList<RelClusterWritable> results;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        outpath = new HDFSPath(conf, conf.get("output"));
    }

    @Override
    public void reduce(Setting key, Iterable<RelClusterWritable> values, Context context) throws IOException, InterruptedException {
        if (trecfile == null) {
            Datafile df = outpath.getFile(sprintf("results.%d.%d.%d.%d", (int) (100 * key.gainratio), (int) (10 * key.hours), key.length, key.topk));
            log.info("%s", df.getFilename());
            trecfile = new TrecFile(df);
            trecfile.openWrite();
            df = outpath.getFile(sprintf("results.%d.%d.%d.%d.titles", (int) (100 * key.gainratio), (int) (10 * key.hours), key.length, key.topk));
            outclusterfile = new RelClusterFile(df);
            outclusterfile.openWrite();
            results = new ArrayList();
            log.info("%f %f %d %d %d", key.gainratio, key.hours, key.length, key.topk, key.topicid);
        }
        for (RelClusterWritable t : values) {
            RelClusterWritable nt = new RelClusterWritable();
            nt.clusterid = t.clusterid;
            nt.creationtime = t.creationtime;
            nt.documentid = t.documentid;
            nt.domain = t.domain;
            nt.nnid = t.nnid;
            nt.nnscore = t.nnscore;
            nt.row = t.row;
            nt.title = t.title;
            nt.urlid = t.urlid;
            results.add(nt);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (results != null && results.size() > 0) {
            Collections.sort(results, new Sorter());

            for (RelClusterWritable t : results) {
                log.info("%d %s", t.creationtime, t.documentid);
                t.write(outclusterfile);
                record.document = t.documentid;
                record.timestamp = t.creationtime;
                record.topic = t.clusterid;
                record.sentence = t.row;
                record.write(trecfile);
            }
            trecfile.closeWrite();
            outclusterfile.closeWrite();
        }
    }

    class Sorter implements Comparator<RelClusterWritable> {

        @Override
        public int compare(RelClusterWritable o1, RelClusterWritable o2) {
            long comp = o1.clusterid - o1.clusterid;
            if (comp == 0) {
                comp = o1.creationtime - o2.creationtime;
            }
            return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
        }

    }
}
