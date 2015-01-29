package kba9trec;

import KNN.Cluster;
import KNN.Edge;
import KNN.Score;
import KNN.Stream;
import KNN.Url;
import KNN.UrlD;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.lib.BoolTools;
import io.github.repir.tools.lib.MathTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public class RetrieveTopParam {

    private static final Log log = new Log(RetrieveTopParam.class);
    TrecWritable record = new TrecWritable();
    TrecFile outfile;
    RelClusterWritable recordcluster = new RelClusterWritable();
    RelClusterFile outclusterfile;
    ArrayList<byte[]> emitted = new ArrayList();
    boolean whitespace[] = BoolTools.whitespace();
    Model termsrelevant = new Model();
    Model termsnotrelevant = new Model();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    DefaultTokenizer deftokenizer = new DefaultTokenizer();
    double avgbase, urltobase;
    boolean watch;

    public RetrieveTopParam(Datafile out, double avgbase, double urltobase) {
        outfile = new TrecFile(out);
        outfile.openWrite();
        outclusterfile = new RelClusterFile(out.getDir().getFile(out.getFilename() + ".titles"));
        outclusterfile.openWrite();
        this.avgbase = avgbase;
        this.urltobase = urltobase;
    }

    public void grep(TopicWritable topic, Datafile in) {
        termsrelevant = new Model();
        termsnotrelevant = new Model();
        emitted = new ArrayList();
        HashSet<String> termsseen = new HashSet();
        HashSet<String> queryterms = new HashSet(tokenizer.tokenize(topic.query));
        termsrelevant.add(queryterms);
        HashMap<Integer, ArrayList<StreamClusterWritable>> map = new HashMap();
        StreamClusterFile cf = new StreamClusterFile(in);
        cf.setBufferSize(100000000);
        for (StreamClusterWritable cw : cf) {
            Cluster c = createCluster(cw);
            UrlD u = (UrlD) c.getUrls().get(c.getUrls().size() - 1);
//            if (u.getID() == 608265214) {
//                watch = true;
//            }
            if (u.getCreationTime() >= topic.start
                    && u.getCreationTime() <= topic.end
                    && u.getFeatures().size() > 0
                    && u.getFeatures().size() < 50) {
                double clusterbasescore = c.getAvgBaseScore();
                double urlbasescore = score(u);
                if (clusterbasescore >= avgbase && urlbasescore >= urltobase) {
                    String title = cleanedTitle(u.getTitle());
                    byte[] titlebytes = title.getBytes();
                    if (containsOldInfo(u) && !alreadySeen2(titlebytes)) {
                        out(topic.id, u, title);
                        emitted.add(titlebytes);
                    }
                }
                if (watch) {
                    close();
                    log.exit();
                }
                addInfo(queryterms, c);
            }
        }
    }

    public boolean containsOldInfo(UrlD u) {
        return termsrelevant.p(u.getFeatures()) < termsnotrelevant.p(u.getFeatures());
    }
    
    public boolean alreadySeen(byte[] title) {
        int minend = title.length - 1;
        int maxstart = 0;
        for (byte[] past : emitted) {
            int start = 0;
            int end = MathTools.min(title.length, past.length, minend);
            for (; start < end
                    && title[start] == past[start]; start++);
            int endnow = title.length - 1;
            int endpast = past.length - 1;
            for (; endnow > maxstart && endpast > 0
                    && title[endnow] == past[endpast]; endnow--, endpast--);
            if (watch) {
                log.info("%s %s %d %d %d %d", new String(title), new String(past), maxstart, start, endnow, minend);
            }
            if (start > maxstart || endnow < minend) {
                if (start > maxstart) {
                    maxstart = start;
                }
                if (endnow < minend) {
                    minend = endnow;
                }
                int countwhitespace = 0;
                for (int i = maxstart; i < minend;) {
                    if (whitespace[title[i] & 0xff]) {
                        countwhitespace++;
                        for (i++; i < end && whitespace[title[i] & 0xff]; i++);
                    } else {
                        i++;
                    }
                }
                if (countwhitespace < 1) {
                    //log.info("OK!");
                    return true;
                }
            }
        }
        if (watch) {
            log.info("OEPS!");
        }
        return false;
    }

    public String cleanedTitle(String title) {
        ArrayList<String> tokenize = deftokenizer.tokenize(title);
        StringBuilder sb = new StringBuilder();
        for (String t : tokenize) {
            if (t.length() > 0) {
                sb.append(' ').append(t);
            }
        }
        return sb.deleteCharAt(0).toString();
    }

    public boolean alreadySeen2(byte[] title) {
        for (byte[] past : emitted) {
            if (past.length >= title.length) {
                int lenience = past.length - title.length;
                LOOP:
                for (int start = 0; start <= lenience; start++) {
                    for (int i = 0; i < title.length; i++) {
                        if (title[i] != past[i + start]) {
                            continue LOOP;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public boolean alreadySeen3(byte[] title) {
        for (byte[] past : emitted) {
            if (past.length >= title.length) {
                int lenience = past.length - title.length;
                LOOP:
                for (int start = 0; start <= lenience; start++) {
                    for (int i = 0; i < title.length; i++) {
                        if (title[i] != past[i + start]) {
                            continue LOOP;
                        }
                    }
                    return true;
                }
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

    public void addInfo(HashSet<String> query, Cluster<UrlD> c) {
        for (Url base : c.getUrls()) {
            if (containsQueryTerm(query, (UrlD) base)) {
                this.termsrelevant.add(base.getFeatures());
            } else {
                this.termsnotrelevant.add(base.getFeatures());
            }
        }
    }

    public boolean containsQueryTerm(HashSet<String> query, UrlD u) {
        for (String term : u.getFeatures()) {
            if (query.contains(term)) {
                return true;
            }
        }
        return false;
    }

    public void out(int topic, Url u, String title) {
        record.document = ((UrlD) u).getDocumentID();
        record.timestamp = u.getCreationTime();
        record.topic = topic;
        record.sentence = ((UrlD) u).sentence;
        record.write(outfile);
        recordcluster.clusterid = topic;
        recordcluster.creationtime = u.getCreationTime();
        recordcluster.domain = u.getDomain();
        recordcluster.nnid = u.getNN();
        recordcluster.nnscore = u.getScore();
        recordcluster.title = title;
        recordcluster.urlid = u.getID();
        recordcluster.documentid = ((UrlD) u).getDocumentID();
        recordcluster.row = ((UrlD) u).sentence;
        recordcluster.write(outclusterfile);
    }

    public Cluster createCluster(StreamClusterWritable cluster) {
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
        outclusterfile.closeWrite();
    }

    public static HashMap<Integer, TopicWritable> getTopics(String topicfile) {
        Datafile df = new Datafile(topicfile);
        TopicFile tf = new TopicFile(df);
        return tf.getMap();
    }

    class Model extends HashMapInt<String> {
        double termsseen = 0;
        
        public void add(Collection<String> terms) {
            for (String term : terms)
                add(term, 1);
            termsseen += terms.size();
        }
        
        public double p(Collection<String> terms) {
            double p = 1;
            for (String term : terms) {
                Integer freq = get(term);
                if (freq != null)
                    p *= (1 - freq / termsseen);
            }
            return p;
        }
    }
    
    public static void main(String args[]) throws IOException {
        Conf conf = new Conf(args, "-i input -o output -t topicfile -b avgbase -u urltobase");
        
        HDFSPath inpath = new HDFSPath(conf, conf.get("input"));

        Datafile fout = new Datafile(conf.get("output"));
        double avgbase = conf.getDouble("avgbase", 0.5);
        double urltobase = conf.getDouble("urltobase", 0.5);
        RetrieveTopParam grep = new RetrieveTopParam(fout, avgbase, urltobase);
        HashMap<Integer, TopicWritable> topics = getTopics(conf.get("topicfile"));
        for (TopicWritable t : topics.values()) {
            log.info("%s", inpath.getFilenames());
            ArrayList<String> filenames = inpath.getFilenames("*." + t.id + "$");
            log.info("topic %d %s", t.id, filenames);
            if (filenames.size() == 1 && !filenames.get(0).equals("topic.6")) {
                Datafile dfin = inpath.getFile(filenames.get(0));
                grep.grep(t, dfin);
            }
        }
        grep.close();
    }
}
