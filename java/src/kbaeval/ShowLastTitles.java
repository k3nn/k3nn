package kbaeval;

import KNN.Cluster;
import KNN.Edge;
import KNN.Score;
import KNN.Stream;
import KNN.Url;
import KNN.UrlD;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.lib.ArrayTools;
import io.github.repir.tools.lib.BoolTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ShowLastTitles {

    private static final Log log = new Log(ShowLastTitles.class);
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    DefaultTokenizer deftokenizer = new DefaultTokenizer();
    int termsseen = 0;
    HashMapInt<String> terms = new HashMapInt();
    ArrayList<byte[]> emitted = new ArrayList();
    double avgbase, urltobase;

    public ShowLastTitles(double avgbase, double urltobase) {
        this.avgbase = avgbase;
        this.urltobase = urltobase;
    }

    public void grep(TopicWritable topic, Datafile in) {
        HashSet<String> queryterms = new HashSet(tokenizer.tokenize(topic.query));
        HashMap<Integer, ArrayList<StreamClusterWritable>> map = new HashMap();
        StreamClusterFile cf = new StreamClusterFile(in);
        for (StreamClusterWritable cw : cf) {
            Cluster c = createCluster(cw);
            UrlD u = (UrlD) c.getUrls().get(c.getUrls().size() - 1);
            if (u.getCreationTime() >= topic.start && 
                    u.getCreationTime() <= topic.end &&
                    u.getFeatures().size() < 30) {
                addInfo(queryterms, c);
                double clusterbasescore = c.getAvgBaseScore();
                double urlbasescore = score(u);
                boolean containsOldInfo = containsOldInfo(queryterms, u);
                if (clusterbasescore >= avgbase && urlbasescore >= urltobase) {
                    if (containsOldInfo) {
                        byte[] titlebytes = cleanedTitle(u.getTitle());
                        log.printf("%b %f %f %s", alreadySeen2(titlebytes), clusterbasescore, urlbasescore, u.getTitle());
                        emitted.add(titlebytes);
                    }
                }
            }
        }
        this.printTermsSeen();
    }
    
    public byte[] cleanedTitle(String title) {
        ArrayList<String> tokenize = deftokenizer.tokenize(title);
        StringBuilder sb = new StringBuilder();
        for (String t : tokenize) {
            if (t.length() > 0)
               sb.append(' ').append(t);
        }
        return sb.deleteCharAt(0).toString().getBytes();
    }
    
    public int max(HashSet<String> title) {
        int max = 0;
        for (String term : title) {
            Integer freq = terms.get(term);
            if (freq != null && freq > max) {
                max = freq;
            }
        }
        return max;
    }

    public int min(HashSet<String> title) {
        int min = Integer.MAX_VALUE;
        for (String term : title) {
            Integer freq = terms.get(term);
            if (freq == null && freq < min) {
                return 0;
            } else {
                min = freq;
            } 
        }
        return min;
    }

    public void addInfo(HashSet<String> query, Cluster<UrlD> c) {
        for (Url base : c.getBase()) {
            if (containsOldInfo(query, (UrlD) base)) {
                for (String term : base.getFeatures()) {
                    if (query.contains(term)) {
                        this.addInfo((UrlD) base);
                        break;
                    }
                }
            }
        }
    }

    public void addInfo(UrlD u) {
        for (String term : u.getFeatures()) {
            terms.add(term, 1);
            this.termsseen++;
        }
    }

    public boolean containsOldInfo(HashSet<String> query, UrlD u) {
        boolean containsOldInfo = false;
        double score = 0;
        for (String term : u.getFeatures()) {
            if (query.contains(term)) {
                containsOldInfo = true;
                break;
            }
            Integer freq = terms.get(term);
            if (freq > 0) {
                score += freq / (double) this.termsseen;
            }
        }
        if (score > 0.05) {
            containsOldInfo = true;
        }
        if (containsOldInfo) {
            addInfo(u);
        }
        return containsOldInfo;
    }

    public boolean alreadySeen(byte []title) {
        int minend = title.length;
        int maxstart = 0;
        for (byte[] past : emitted) {
            int start = 0;
            for (start = 0; start < minend && 
                    start < past.length && 
                    title[start] == past[start]; start++);
            if (start > maxstart)
                maxstart = start;
            int end = title.length -1;
            int endpast = past.length - 1;
            for (; end > maxstart && endpast > 0 &&
                    title[end] == past[endpast]; end--, endpast--);
            if (end < minend)
                minend = end;
            if (minend <= start)
                return true;
        }
        int countwhitespace = 0;
        for (int i = maxstart; i < minend;) {
            if (BoolTools.whitespace()[title[i] & 0xff]) {
                countwhitespace++;
                for (i++; i < minend && BoolTools.whitespace()[title[i] & 0xff]; i++);
            } else 
                i++;
        }
        return countwhitespace < 2;
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
    
    public void printTermsSeen() {
        ArrayMap<Double, String> sorted = new ArrayMap();
        for (Map.Entry<String, Integer> entry : terms.entrySet()) {
            sorted.add(entry.getValue() / (double) this.termsseen, entry.getKey());
        }
        sorted.descending();
        for (int i = 0; i < 100 && i < sorted.size(); i++) {
            log.printf("%f %s", sorted.get(i).getKey(), sorted.get(i).getValue());
        }
    }

    public double score(UrlD u) {
        double score = 0;
        int count = 0;
        for (Url basenode : ((Cluster<UrlD>)u.getCluster()).getBase()) {
            if (basenode != u) {
                score += score(u, basenode);
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

    public static HashMap<Integer, TopicWritable> getTopics(Conf conf, String label) {
        Datafile df = new Datafile(conf.get(label));
        TopicFile tf = new TopicFile(df);
        return tf.getMap();
    }

    public static void main(String args[]) throws IOException {
        Conf conf = new Conf(args, "-t topics -i input -b avgbase -u urltobase");
        HashMap<Integer, TopicWritable> topics = getTopics(conf, "topics");
        Datafile dfin = new Datafile(conf, conf.get("input"));
        String topicstr = dfin.getFilename().substring(dfin.getFilename().indexOf('.') + 1);
        int topic = Integer.parseInt(topicstr);

        double avgbase = conf.getDouble("avgbase", 0.5);
        double urltobase = conf.getDouble("urltobase", 0.5);
        ShowLastTitles grep = new ShowLastTitles(avgbase, urltobase);
        grep.grep(topics.get(topic), dfin);
    }
}
