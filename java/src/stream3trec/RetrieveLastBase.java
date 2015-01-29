package stream3trec;

import KNN.Cluster;
import KNN.Edge;
import KNN.Score;
import KNN.Stream;
import KNN.Url;
import KNN.UrlD;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public class RetrieveLastBase {

    private static final Log log = new Log(RetrieveLastBase.class);
    TrecWritable record = new TrecWritable();
    TrecFile outfile;
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    HashMap<Integer, Long> queryseen = new HashMap();
    double avgbase, urltobase;

    public RetrieveLastBase(Datafile out, double avgbase, double urltobase) {
        outfile = new TrecFile(out);
        outfile.openWrite();
        this.avgbase = avgbase;
        this.urltobase = urltobase;
    }

    public void grep(TopicWritable topic, Datafile in) {
        HashSet<String> termsseen = new HashSet();
        ArrayList<String> query = tokenizer.tokenize(topic.query);
        HashMap<Integer, ArrayList<StreamClusterWritable>> map = new HashMap();
        StreamClusterFile cf = new StreamClusterFile(in);
        for (StreamClusterWritable cw : cf) {
            Cluster c = createCluster(cw);
            UrlD u = (UrlD) c.getUrls().get(c.getUrls().size() - 1);
            if (c.getAvgBaseScore() >= avgbase && score(u) >= urltobase) {
                //if (!termsseen.containsAll(u.getFeatures())) {
                    termsseen.addAll(u.getFeatures());
                    out(topic.id, u);
                //}
            }
        }
    }

    public boolean valid(ArrayList<String> query, UrlD u) {
        for (String term : query) {
            if (u.getFeatures().contains(term)) {
                return score(u) >= urltobase && ((UrlD) u).sentence < 15;
            }
        }
        return false;
    }

    public double score(Url u) {
        double score = 0;
        int count = 0;
        for (Url v : ((Cluster<UrlD>)u.getCluster()).getBase()) {
            if (v != u) {
                score += score(u, v);
                count++;
            }
        }
        return score / count;
    }

    public double score(Url u, Url v) {
        double score = Score.timeliness(u.getCreationTime(), v.getCreationTime());
        if (score > 0) {
            score *= Score.cossim(((UrlD) u).getFeatures(), ((UrlD) v).getFeatures());
        }
        return score;
    }

    public void out(int topic, Url u) {
        record.document = ((UrlD) u).getDocumentID();
        record.timestamp = u.getCreationTime();
        record.topic = topic;
        record.write(outfile);
    }

    public Long querySeen(ArrayList<String> query, Cluster<UrlD> cluster) {
        Long seen = queryseen.get(cluster.getID());
        if (seen == null) {
            long creationtime = 0;
            for (Url u : cluster.getBase()) {
                if (u.getCreationTime() > creationtime) {
                    creationtime = u.getCreationTime();
                }
            }
            for (Url u : cluster.getBase()) {
                HashSet<String> title = new HashSet(tokenizer.tokenize(u.getTitle()));
                if (title.containsAll(query)) {
                    seen = creationtime;
                    break;
                }
            }
            if (seen == null) {
                creationtime = Long.MAX_VALUE;
                for (Url u : cluster.getUrls()) {
                    if (!cluster.getBase().contains(u)) {
                        HashSet<String> title = ((UrlD) u).getFeatures();
                        if (title.containsAll(query)) {
                            creationtime = Math.min(creationtime, u.getCreationTime());
                        }
                    }
                }
                if (creationtime < Long.MAX_VALUE) {
                    seen = creationtime;
                }
            }
        } else {
            Url u = cluster.getUrls().get(cluster.getUrls().size() - 1);
            if (u.getCreationTime() < seen) {
                HashSet<String> title = new HashSet(tokenizer.tokenize(u.getTitle()));
                if (title.containsAll(query)) {
                    seen = u.getCreationTime();
                }
            }
        }
        queryseen.put(cluster.getID(), seen);
        return seen;
    }

    public Cluster<UrlD> createCluster(StreamClusterWritable cluster) {
        Stream<UrlD> s = new Stream();
        for (UrlWritable r : cluster.urls) {
            HashSet<String> title = new HashSet(tokenizer.tokenize(r.title));
            UrlD u = new UrlD(r.urlid, r.domain, r.title, title, r.creationtime, r.getUUID(), r.row);
            s.urls.put(u.getID(), u);
        }
        Cluster c = s.createCluster(cluster.clusterid);
        for (UrlWritable r : cluster.urls) {
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
        Conf conf = new Conf(args, "-i input -o output -t topicfile -b avgbase -u urltobase");
        HDFSPath inpath = new HDFSPath(conf, conf.get("input"));

        Datafile fout = new Datafile(conf.get("output"));
        double avgbase = conf.getDouble("avgbase", 0.5);
        double urltobase = conf.getDouble("urltobase", 0.5);
        RetrieveLastBase grep = new RetrieveLastBase(fout, avgbase, urltobase);
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
