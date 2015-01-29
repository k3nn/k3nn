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
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.extract.modules.RemoveStopwordsInquery;
import io.github.repir.tools.extract.modules.RemoveStopwordsSmart;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.lib.BoolTools;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.xerial.snappy.Snappy;

public class RetrieveTop {

    private static final Log log = new Log(RetrieveTop.class);
    TrecWritable record = new TrecWritable();
    TrecFile outfile;
    RelClusterWritable recordcluster = new RelClusterWritable();
    RelClusterFile outclusterfile;
    HashMap<String, HashSet<String>> bigrams = new HashMap();
    ArrayList<byte[]> emitted = new ArrayList();
    ByteBuffer emittedbuffer = ByteBuffer.allocateDirect(10000000);
    ByteBuffer dummybuffer = ByteBuffer.allocateDirect(1000000);
    int emittedcompressedsize = 0;
    double emittedcompressratio = 0;
    boolean whitespace[] = BoolTools.whitespace();
    Model termsrelevant = new Model();
    Model termsnotrelevant = new Model();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    DefaultTokenizer deftokenizer = new DefaultTokenizer();
    double avgbase, urltobase;
    int minUnseenBigrams = 3;
    double seenbigramratio = 0;
    int supporttype = 1;
    int minsupport = 2;
    boolean watch;

    public RetrieveTop(Datafile out, double avgbase, double urltobase) {
        outfile = new TrecFile(out);
        outfile.openWrite();
        outclusterfile = new RelClusterFile(out.getDir().getFile(out.getFilename() + ".titles"));
        outclusterfile.openWrite();
        this.avgbase = avgbase;
        this.urltobase = urltobase;
        deftokenizer.addEndPipeline(RemoveStopwordsInquery.class);
        //deftokenizer.addEndPipeline(StemTokens.class);
    }

    public void grep(TopicWritable topic, Datafile in) throws IOException {
        emittedbuffer = ByteBuffer.allocateDirect(10000000);
        dummybuffer = ByteBuffer.allocateDirect(1000000);
        emittedcompressedsize = 0;
        emittedcompressratio = 0;
        termsrelevant = new Model();
        termsnotrelevant = new Model();
        emitted = new ArrayList();
        bigrams = new HashMap();
        HashSet<String> termsseen = new HashSet();
        HashSet<String> queryterms = new HashSet(tokenizer.tokenize(topic.query));
        termsrelevant.add(queryterms);
        HashMap<Integer, ArrayList<StreamClusterWritable>> map = new HashMap();
        StreamClusterFile cf = new StreamClusterFile(in);
        cf.setBufferSize(100000000);
        for (StreamClusterWritable cw : cf) {
            Cluster c = createCluster(cw);
            UrlD u = (UrlD) c.getUrls().get(c.getUrls().size() - 1);
            if (u.getID() == -604701415) {
                watch = true;
                double clusterbasescore = c.getAvgBaseScore();
                double urlbasescore = score(u);
                double relevant = termsrelevant.preport(u.getFeatures());
                double notrelevant = termsnotrelevant.preport(u.getFeatures());
                log.info("%d clusterbase %f urlbase %f relevant %f notrelevant %f",
                        u.getID(), clusterbasescore, urlbasescore, relevant, notrelevant);
            }
            if (u.getCreationTime() >= topic.start
                    && u.getCreationTime() <= topic.end
                    && u.getFeatures().size() > 2
                    && u.getFeatures().size() < 40) {
                double clusterbasescore = c.getAvgBaseScore();
                double urlbasescore = score(u);
                if (clusterbasescore >= avgbase && urlbasescore >= urltobase) {
                    ArrayList<String> titletokens = deftokenizer.tokenize(u.getTitle());
                    String title = cleanedTitle(titletokens);
                    byte[] titlebytes = title.getBytes();
                    //Tuple2<Integer, Double> containsNewInfo = containsNewInfo(titlebytes);
                    //log.info("newinfo %f %f %b %s", containsNewInfo.value2, 
                    //        emittedcompressratio, containsNewInfo.value2 > emittedcompressratio, title);
                    //boolean oldinfo = containsOldInfo(u) || containsOldInfo(c);
                    boolean oldinfo = containsQuery(u, queryterms) || containsQuery(c, queryterms);
                    ArrayMap<String, String> unseenBigrams = unseenBigrams(titletokens);
                    double entropy = termsrelevant.entropy(queryterms);
                    double ratio = unseenBigrams.size() == 0?0:1 - unsupported(u, unseenBigrams, queryterms).size() / (double)unseenBigrams.size();
                    boolean newSupportedInfo = unseenBigrams.size() >= minUnseenBigrams
                            && ratio >= 0.5;
                    //if (oldinfo && containsnewinfo) {
                    log.info("%b %b %f %s %s", oldinfo, newSupportedInfo, ratio, unseenBigrams, u.getTitle());
                    //}
                    if (oldinfo && newSupportedInfo) {
                        out(topic.id, u, u.getTitle());
                        emitted.add(titlebytes);
                        for (int i = 0; i < titletokens.size() - 1; i++) {
                            HashSet<String> set = bigrams.get(titletokens.get(i));
                            if (set == null) {
                                set = new HashSet();
                                bigrams.put(titletokens.get(i), set);
                            }
                            for (int j = 0; j < titletokens.size(); j++) {
                                if (j != i) {
                                    set.add(titletokens.get(j));
                                }
                            }
                        }
                        //log.info("bigrams %s", bigrams);
                        //do {
                        //    addEmittedBuffer(titlebytes);
                        //} while (emittedcompressratio > 0.6);
                    }
                    if (containsQuery(u, queryterms)) {
                        addInfo(queryterms, c);
                    }
                }
                if (watch) {
                    close();
                    log.info("%s", this.termsrelevant);
                    log.exit();
                }
            }
        }
    }
    
    public ArrayMap<String, String> unsupported(Url u, ArrayMap<String, String> bigrams, HashSet<String> queryterms) {
        //log.info("checkSupport %s", bigrams);
        ArrayMap<String, String> unsupported = new ArrayMap();
        LOOP:
        for (Map.Entry<String, String> bigram : bigrams) {
            int support = 0;
            switch (supporttype) {
                case 0: // all urls
                    for (Url v : ((Cluster<UrlD>)u.getCluster()).getUrls()) {
                        //if (containsQuery(v, queryterms) || containsQuery(u.getCluster(), queryterms)) {
                            if (v.getDomain() != u.getDomain() && v.getFeatures().contains(bigram.getKey()) && v.getFeatures().contains(bigram.getValue())) {
                                support++;
                            }
                        //}
                    }
                case 1: // base only
                    for (Url v : ((Cluster<UrlD>)u.getCluster()).getBase()) {
                        if (v.getDomain() != u.getDomain() && v.getFeatures().contains(bigram.getKey()) && v.getFeatures().contains(bigram.getValue())) {
                            support++;
                        }
                    }
                case 2: // sufficient coherence
                    for (Url v : ((Cluster<UrlD>)u.getCluster()).getUrls()) {
                        if (v.getDomain() != u.getDomain() && score(u, v) > urltobase && v.getFeatures().contains(bigram.getKey()) && v.getFeatures().contains(bigram.getValue())) {
                            support++;
                        }
                    }
            }
            if (support < minsupport) {
                unsupported.add(bigram);
            }
        }
        //log.info("unsupported %s", unsupported);
        return unsupported;
    }

    public void addEmittedBuffer(byte[] title) throws IOException {
        emittedbuffer.put((byte) 0);
        emittedbuffer.put(title);
        emittedcompressedsize = compress();
        emittedcompressratio = emittedcompressedsize / (double) emittedbuffer.position();
    }

    public int compress() throws IOException {
        ByteBuffer duplicate = emittedbuffer.duplicate();
        duplicate.flip();
        dummybuffer.rewind();
        return Snappy.compress(duplicate, dummybuffer);
    }

    public Tuple2<Integer, Double> containsNewInfo(byte[] title) throws IOException {
        emittedbuffer.mark();
        emittedbuffer.put((byte) 0);
        emittedbuffer.put(title);
        int compressedsize = compress();
        double ratio = compressedsize / (double) emittedbuffer.position();
        Tuple2<Integer, Double> result = new Tuple2(compressedsize, ratio);
        emittedbuffer.reset();
        return result;
    }

    public boolean containsOldInfo(UrlD u) {
        double p1 = termsrelevant.p(u.getFeatures());
        if (watch) {
            log.info(p1);
        }
        return p1 < 0.95;
    }

    public boolean containsOldInfo(Cluster<UrlD> c) {
        for (Url u : c.getUrls()) {
            if (containsOldInfo((UrlD) u)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsQuery(Url u, HashSet<String> query) {
        for (String term : query) {
            if (u.getFeatures().contains(term)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsQuery(Cluster<UrlD> c, HashSet<String> query) {
        for (Url u : c.getBase()) {
            if (containsQuery(u, query)) {
                return true;
            }
        }
        return false;
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

    public ArrayMap<String, String> unseenBigrams(ArrayList<String> title) {
        ArrayMap<String, String> unseen = new ArrayMap();
        for (int i = 0; i < title.size() - 1; i++) {
            HashSet<String> set = bigrams.get(title.get(i));
            if (set == null || !set.contains(title.get(i + 1))) {
                unseen.add(title.get(i), title.get(i + 1));
            }
        }
        return unseen;
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

    public String cleanedTitle(ArrayList<String> tokenize) {
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
            if (v.getDomain() != u.getDomain()) {
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
                //this.termsnotrelevant.add(base.getFeatures());
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

    public Cluster<UrlD> createCluster(StreamClusterWritable cluster) {
        Stream<UrlD> s = new Stream();
        for (UrlWritable r : cluster.urls) {
            HashSet<String> title = new HashSet(tokenizer.tokenize(r.title));
            UrlD u = new UrlD(r.urlid, r.domain, r.title, title, r.creationtime, r.getUUID(), r.row);
            s.urls.put(u.getID(), u);
        }
        Cluster<UrlD> c = s.createCluster(cluster.clusterid);
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
            for (String term : terms) {
                add(term, 1);
            }
            termsseen += terms.size();
        }

        public double p(Collection<String> terms) {
            double p = 1;
            for (String term : terms) {
                Integer freq = get(term);
                if (freq != null) {
                    p *= (1 - freq / termsseen);
                }
                if (watch) {
                    log.info("p %s %f", term, p);
                }
            }
            return p;
        }

        public double entropy(Collection<String> terms) {
            double e = 0;
            for (String term : terms) {
                Integer freq = get(term);
                if (freq == null) {
                    freq = 0;
                }
                double p = (1 + freq) / (termsseen + terms.size());
                e -= p * MathTools.log2(p);
            }
            return e;
        }

        public double preport(Collection<String> terms) {
            double p = 1;
            for (String term : terms) {
                Integer freq = get(term);
                if (freq != null) {
                    log.info("%s %d %f %f", term, freq, termsseen, 1 - freq / termsseen);
                    p *= (1 - freq / termsseen);
                }
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
        RetrieveTop grep = new RetrieveTop(fout, avgbase, urltobase);
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
