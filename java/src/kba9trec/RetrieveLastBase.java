package kba9trec;

import kbaeval.TrecWritable;
import kbaeval.TrecFile;
import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Url;
import KNN.UrlM;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public class RetrieveLastBase {

    private static final Log log = new Log(RetrieveLastBase.class);
    TrecWritable record = new TrecWritable();
    TrecFile outfile;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();

    public RetrieveLastBase(Datafile out) {
        outfile = new TrecFile(out);
        outfile.openWrite();
    }

    public void grep(TopicWritable topic, Datafile in) {
        HashMap<Integer, ArrayList<RelClusterWritable>> map = new HashMap();
        RelClusterFile cf = new RelClusterFile(in);
        for (RelClusterWritable cw : cf) {
            if (cw.clusterid > -1) {
                ArrayList<RelClusterWritable> list = map.get(cw.clusterid);
                if (list == null) {
                    list = new ArrayList();
                    map.put(cw.clusterid, list);
                }
                list.add(cw);
            }
        }
        for (Map.Entry<Integer, ArrayList<RelClusterWritable>> entry : map.entrySet()) {
            long creationtime = querySeen(topic.query, entry.getValue());
            log.info("%d", creationtime);
            Cluster c = select(entry.getValue());
            RelClusterWritable r = get(entry.getValue(), lastBase(c));
            for (RelClusterWritable w : entry.getValue()) {
                if (w.creationtime >= r.creationtime && w.creationtime > creationtime) {
                    out(topic.id, w);
                }
            }
        }
    }

    public void out(int topic, RelClusterWritable w) {
        record.document = w.documentid;
        record.timestamp = w.creationtime;
        record.topic = topic;
        record.write(outfile);
        log.info("%d %d %s", topic, w.creationtime, w.title);
    }

    public long querySeen(String query, ArrayList<RelClusterWritable> list) {
        ArrayList<String> queryterms = tokenizer.tokenize(query);
        long creationtime = Long.MAX_VALUE;
        for (RelClusterWritable r : list) {
            if (r.creationtime < creationtime) {
                HashSet<String> title = new HashSet(tokenizer.tokenize(r.title));
                if (title.containsAll(queryterms)) {
                    creationtime = r.creationtime;
                }
            }
        }
        return creationtime;
    }

    public Cluster select(ArrayList<RelClusterWritable> list) {
        Stream<UrlM> s = new Stream();
        for (RelClusterWritable r : list) {
            UrlM u = new UrlM(r.urlid, r.domain, r.creationtime, 0);
            s.urls.put(u.getID(), u);
        }
        Cluster c = s.createCluster(list.get(0).clusterid);
        for (RelClusterWritable r : list) {
            Url url = s.urls.get(r.urlid);
            ArrayList<Integer> nn = getNN(r.nnid);
            ArrayList<Double> score = getScores(r.nnscore);
            for (int i = 0; i < nn.size(); i++) {
                Url u = s.urls.get(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                url.add(e);
            }
            url.setCluster(c);
        }
        c.setBase(Cluster.getBase(c.getUrls()));
        return c;
    }

    public int lastBase(Cluster<UrlM> c) {
        Url max = null;
        for (Url u : c.getBase()) {
            if (max == null || max.getCreationTime() < u.getCreationTime()) {
                max = u;
            }
        }
        if (max == null) {
            log.info("cluster no base: %s", c.toString());
        }
        return max.getID();
    }

    public RelClusterWritable get(ArrayList<RelClusterWritable> list, int id) {
        for (RelClusterWritable r : list) {
            if (r.urlid == id) {
                return r;
            }
        }
        return null;
    }

    public ArrayList<Integer> getNN(String nn) {
        ArrayList<Integer> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Integer.parseInt(n));
        }
        return result;
    }

    public ArrayList<Double> getScores(String nn) {
        ArrayList<Double> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Double.parseDouble(n));
        }
        return result;
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
        RetrieveLastBase grep = new RetrieveLastBase(fout);
        HashMap<Integer, TopicWritable> topics = getTopics(conf, "topicfile");
        for (TopicWritable t : topics.values()) {
            log.info("%s", inpath.getFilenames());
            ArrayList<String> filenames = inpath.getFilenames("*." + t.id + "$");
            log.info("topic %d %s", t.id, filenames);
            if (filenames.size() == 1) {
                Datafile dfin = inpath.getFile(filenames.get(0));
                grep.grep(t, dfin);
            }
        }
        grep.close();
    }
}
