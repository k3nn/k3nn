package stream5Retrieve;

import KNN.NodeD;
import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
import io.github.htools.io.HDFSPath;
import static io.github.htools.lib.PrintTools.sprintf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import stream5Retrieve.RetrieveJob.Setting;

/**
 *
 * @author jeroen
 */
public class RetrieveReducer extends Reducer<Setting, MatchingClusterNodeWritable, NullWritable, NullWritable> {

    public static final Log log = new Log(RetrieveReducer.class);
    Configuration conf;
    HDFSPath outpath;
    TrecFile trecfile;
    TrecWritable record = new TrecWritable();
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();
    MatchingClusterNodeFile outclusterfile;
    ArrayList<MatchingClusterNodeWritable> results;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        outpath = new HDFSPath(conf, conf.get("output"));
    }

    @Override
    public void reduce(Setting key, Iterable<MatchingClusterNodeWritable> values, Context context) throws IOException, InterruptedException {
        if (trecfile == null) {
            Datafile df = outpath.getFile(sprintf("results.%d.%d.%d.%d", (int) (100 * key.gainratio), (int) (10 * key.hours), key.length, key.topk));
            log.info("%s", df.getName());
            trecfile = new TrecFile(df);
            trecfile.openWrite();
            df = outpath.getFile(sprintf("results.%d.%d.%d.%d.titles", (int) (100 * key.gainratio), (int) (10 * key.hours), key.length, key.topk));
            outclusterfile = new MatchingClusterNodeFile(df);
            outclusterfile.openWrite();
            results = new ArrayList();
            log.info("%f %f %d %d %d", key.gainratio, key.hours, key.length, key.topk, key.topicid);
        }
        for (MatchingClusterNodeWritable t : values) {
            MatchingClusterNodeWritable nt = new MatchingClusterNodeWritable();
            nt.clusterID = t.clusterID;
            nt.creationTime = t.creationTime;
            nt.documentID = t.documentID;
            nt.domain = t.domain;
            nt.nnid = t.nnid;
            nt.nnscore = t.nnscore;
            nt.sentenceNumber = t.sentenceNumber;
            nt.content = t.content;
            nt.sentenceID = t.sentenceID;
            results.add(nt);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (results != null && results.size() > 0) {
            Collections.sort(results, new Sorter());

            for (MatchingClusterNodeWritable t : results) {
                log.info("%d %s", t.creationTime, t.documentID);
                t.write(outclusterfile);
                record.document = t.documentID;
                record.timestamp = t.creationTime;
                record.topic = t.clusterID;
                record.sentence = t.sentenceNumber;
                record.write(trecfile);
            }
            trecfile.closeWrite();
            outclusterfile.closeWrite();
        }
    }

    class Sorter implements Comparator<MatchingClusterNodeWritable> {

        @Override
        public int compare(MatchingClusterNodeWritable o1, MatchingClusterNodeWritable o2) {
            long comp = o1.clusterID - o1.clusterID;
            if (comp == 0) {
                comp = o1.creationTime - o2.creationTime;
            }
            return (comp < 0) ? -1 : (comp > 0) ? 1 : 0;
        }

    }
}
