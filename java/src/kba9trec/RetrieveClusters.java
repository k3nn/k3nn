package kba9trec;

import kbaeval.TrecWritable;
import kbaeval.TrecFile;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public class RetrieveClusters {

    private static final Log log = new Log(RetrieveClusters.class);
    TrecWritable record = new TrecWritable();
    TrecFile outfile;

    public RetrieveClusters(Datafile out) {
        outfile = new TrecFile(out);
        outfile.openWrite();
    }

    public void grep(int topic, Datafile in) {
        RelClusterFile cf = new RelClusterFile(in);
        for (RelClusterWritable cw : cf) {
            if (cw.clusterid > -1) {
                record.document = cw.documentid;
                record.timestamp = cw.creationtime;
                record.topic = topic;
                record.write(outfile);
            }
        }
    }

    public void close() {
        outfile.closeWrite();
    }

    public static HashMap<Integer, TopicWritable> getTopics(Conf conf, String label) {
        Datafile df = new Datafile(conf.get(label));
        TopicFile tf = new TopicFile(df);
        return tf.getMap();
    }

    public static void main(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile");
        HDFSPath inpath = new HDFSPath(conf, conf.get("input"));

        Datafile fout = new Datafile(conf.get("output"));
        RetrieveClusters grep = new RetrieveClusters(fout);
        HashMap<Integer, TopicWritable> topics = getTopics(conf, "topicfile");
        for (int topicid : topics.keySet()) {
            log.info("%s", inpath.getFilenames());
            ArrayList<String> filenames = inpath.getFilenames("*." + topicid + "$");
            log.info("topic %d %s", topicid, filenames);
            if (filenames.size() == 1) {
                Datafile dfin = inpath.getFile(filenames.get(0));
                grep.grep(topicid, dfin);
            }
        }
        grep.close();
    }
}
