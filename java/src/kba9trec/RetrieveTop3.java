package kba9trec;

import KNN.Cluster;
import KNN.Edge;
import KNN.EdgeIterator;
import KNN.Score;
import KNN.Stream;
import KNN.Url;
import KNN.UrlD;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.collection.TopK;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.MathTools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public abstract class RetrieveTop3 {

    private static final Log log = new Log(RetrieveTop3.class);
    ArrayMap<Double, UrlD> emitteddoc;
    ArrayMap<Double, Collection<String>> emitteddocall;
    HashMap<String, HashSet<String>> emitted = new HashMap();
    ArrayMap<Long, HashSet<String>> emitteddoctime = new ArrayMap();
    HashSet<Integer> docadded;
    //HashSet<Integer> nonreldocadded;
    int emittedcount = 0;
    Model termsrelevant = new Model();
    //Model termsallrelevant = new Model();
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    DefaultTokenizer deftokenizer = new DefaultTokenizer();
    long topicstart;
    long topicend;
    int topicid;
    public int topk = 5;
    //public int comparepos = 0;
    public int maxterms = 20;
    boolean watch;
    public double mingainratio = 0.5;
    public double RelevanceModelHours = 1;
    HashSet<String> queryterms;
    //HashSet<String> revisedquery;
    private int forget = 1 * 60 * 60;

    public abstract void out(int topic, Url u, String title) throws IOException, InterruptedException;

    public void init(int topicid, long topicstart, long topicend, String query) throws IOException {
        this.topicid = topicid;
        this.topicstart = topicstart;
        this.topicend = topicend;
        forget = (int) (RelevanceModelHours * 60 * 60);
        termsrelevant = new Model();
        //termsallrelevant = new Model();
        emitteddoc = new ArrayMap();
        emitteddocall = new ArrayMap();
        emitted = new HashMap();
        emittedcount = 0;
        //comparepos = 0;
        docadded = new HashSet();
        //nonreldocadded = new HashSet();
        queryterms = new HashSet(tokenizer.tokenize(query));
        //revisedquery = new HashSet(queryterms);
        this.emitteddoc(new ArrayList(queryterms));
        emitteddoctime = new ArrayMap();
        termsrelevant.add(queryterms);
        //termsallrelevant.add(queryterms);
        HashMap<Integer, ArrayList<StreamClusterWritable>> map = new HashMap();
    }

    public void stream(StreamClusterWritable writable) throws IOException, InterruptedException {
        Cluster c = createCluster(writable);
        UrlD u = (UrlD) c.getUrls().get(c.getUrls().size() - 1);
        stripCluster(c);
        ArrayList<String> allterms = tokenizer.tokenize(u.getTitle());
        HashSet<String> titleset = new HashSet(allterms);
        ArrayList<String> titletokens = new ArrayList(titleset);
            double clusterbasescore = c.getAvgBaseScore();
            double urlbasescore = score(u);
            boolean contains = c.getUrls().contains(u);
            boolean clusterqualifies = qualifies(c, queryterms);
            boolean urlqualifies = qualify(u, titleset);
        if (false) {
            log.info("%d %b %b %b %s",
                    u.getID(), contains, clusterqualifies, urlqualifies, u.getTitle());
        }
        if (contains && clusterqualifies && urlqualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
            String title = cleanedTitle(titletokens);
            this.removeDoc(u.getCreationTime());
//            if (this.emitteddoctime.size() == 0)
//                comparepos = 0;
            this.updateEmittedDocs();
            double urlrel = this.termsrelevant.cosSim(titletokens);
            //double urlrelall = termsallrelevant.cosSim(titletokens);
            //double urlnotrel = this.termsnotrelevant.cosSim(titletokens);
            boolean topdocument = false;
            double simToBeat = (emitteddoc.size() >= topk)?emitteddoc.getKey(topk - 1):0;
            //this.updateEmittedDocsAll();
            //double simToBeatAll = (emitteddoc.size() >= topk)?emitteddoc.getKey(topk - 1):0;
            if (c.getID() == 164 || c.getID() == 174) {
                if (u.getID() == 1297265410 || u.getID() == 1297265406) {
                    log.info("%s", this.termsrelevant);
                    log.info("%d %d %s", c.getID(), u.getID(), u.getTitle());
                    for (int i = 0; i < emitteddoc.size(); i++) {
                        log.info("%d %f %s", i+1, emitteddoc.getKey(i), emitteddoc.getValue(i).getTitle());
                    }
                    log.info("simtobeat %f sim %f\n", simToBeat, urlrel);
                }
            }
            
            if (emitteddoc.size() < topk) {
                topdocument = true;
            } else {
                topdocument = (urlrel >= simToBeat);
            }
            double gain = this.getSupported(titletokens, c, u);
            //int emitted = this.getEmitted(titletokens);
            int querycombis = this.getEmitted(titletokens, queryterms);
//                if (querycombis < emitted - 1) {
//                    log.info("%d %d %s", emitted, querycombis, title);
//                }
            HashSet<String> titleminusquery = new HashSet(titletokens);
            titleminusquery.removeAll(queryterms);
            int combos = titletokens.size() * (titletokens.size() + 1) / 2;
            int comboshalf = (int) ((titletokens.size() * (1 - mingainratio)) * ((titletokens.size() + 1) * (1 - mingainratio))) / 2;
            int combosfull = (int) (titletokens.size() * (titletokens.size() + 1)) / 2;
            int combos2 = (int)((titleminusquery.size() * (titletokens.size() - 1)) * mingainratio);
            double gainRatio = gain / combos;
            boolean newSupportedInfo = querycombis > 0 && gain >= combos2;
            if (u.getID() == 1460861117)
                log.info("%d %d %d", titleminusquery.size(), titletokens.size() - 1, combos2);
            //log.info("%b %b %d %f %d %f %s", newSupportedInfo, topdocument, querycombis, gain, combos2, urlrel, simToBeat, titletokens);
            if (newSupportedInfo && topdocument) {
                out(topicid, u, u.getTitle());
                emitteddoc(titletokens);
                this.emitteddoc.add(urlrel, u);
                //comparepos++;
            }
            //this.addNonRel(queryterms, c);
            addInfo(queryterms, c);
            //if (containsQuery(u, queryterms)) {
                //this.updateEmittedDocs();
            //    revisedquery = this.reviseQuery(queryterms);
//                    if (!queryterms.containsAll(revisedquery)) {
//                        log.info("REVISED %s", revisedquery);
//                    }
        } else {
            //this.addNonRel(queryterms, c);
        }
    }

    void stripCluster(Cluster c) {
        HashSet<Url> urls = new HashSet();
        HashSet<Url> newurls = c.getBase();
        while (newurls.size() > 0) {
            urls.addAll(newurls);
            HashSet<Url> nextbatch = new HashSet();
            for (Url u : newurls) {
                for (int e = 0; e < u.getEdges(); e++) {
                    Url l = u.getNN(e).getUrl();
                    if (l != null && !urls.contains(l))
                        nextbatch.add(l);
                }
            }
            newurls = nextbatch;
        }
        HashSet<Url> remove = new HashSet(c.getUrls());
        remove.removeAll(urls);
        for (Url u : remove)
            c.remove(u);
    }
    
    boolean qualify(UrlD u, HashSet<String> allterms) {
        return u.getCreationTime() >= topicstart
                && u.getCreationTime() <= topicend
                && allterms.size() <= maxterms;
    }

    public int querycombinations(ArrayList<String> terms, HashSet<String> queryterms) {
        int count = 0;
        for (int i = 0; i < terms.size(); i++) {
            for (int j = i + 1; j < terms.size(); j++) {
                if (queryterms.contains(terms.get(i)) && queryterms.contains(terms.get(j))) {
                    count++;
                }
            }
        }
        return count;
    }

    public HashSet<String> reviseQuery(HashSet<String> queryterms) {
        double p = 0;
        for (String term : queryterms) {
            p = Math.max(p, this.termsrelevant.p(term));
        }
        HashSet<String> highTerms = this.termsrelevant.highTerms(p / 2);
        highTerms.addAll(queryterms);
        return highTerms;
    }

    public void emitteddoc(ArrayList<String> terms) {
//        log.info("emitteddoc %s", terms);
        for (int i = 0; i < terms.size(); i++) {
            for (int j = 0; j < terms.size(); j++) {
                if (i != j) {
                    String t1 = terms.get(i);
                    String t2 = terms.get(j);
                    HashSet<String> list = emitted.get(terms.get(i));
                    if (list == null) {
                        list = new HashSet();
                        emitted.put(terms.get(i), list);
                    }
                    if (!list.contains(terms.get(j))) {
                        list.add(terms.get(j));
                        emittedcount++;
                    }
                }
            }
        }
    }

    public double getSupported(ArrayList<String> terms, Cluster<UrlD> c, Url u) {
        int support = 0;
        for (int i = 0; i < terms.size(); i++) {
            HashSet<String> list = emitted.get(terms.get(i));
            if (list == null) {
                support += (terms.size() - i);
            } else {
                for (int j = i + 1; j < terms.size(); j++) {
                    if (!list.contains(terms.get(j))) {
                        for (Url v : c.getUrls()) {
                            if (v.getDomain() != u.getDomain() && v.getFeatures().contains(terms.get(i)) && v.getFeatures().contains(terms.get(j))) {
                                support++;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return support;
    }

    public int getEmitted(ArrayList<String> terms) {
        int support = 0;
        int e = 0;
        for (int i = 0; i < terms.size(); i++) {
            for (int j = i + 1; j < terms.size(); j++) {
                HashSet<String> list = emitted.get(terms.get(i));
                if (list != null && list.contains(terms.get(j))) {
                    e++;
                }
            }
        }
        return e;
    }

    public int getEmitted(ArrayList<String> terms, HashSet<String> query) {
        int e = 0;
        for (int i = 0; i < terms.size(); i++) {
            for (int j = i + 1; j < terms.size(); j++) {
                HashSet<String> list = emitted.get(terms.get(i));
                if (list != null && list.contains(terms.get(j)) && (query.contains(terms.get(i)) || query.contains(terms.get(j)))) {
                    e++;
                }
            }
        }
        return e;
    }

    public double getUnsupported(ArrayList<String> terms, Cluster<UrlD> c, Url u) {
        int unsupported = 0;
        for (int i = 0; i < terms.size(); i++) {
            for (int j = i + 1; j < terms.size(); j++) {
                if (i != j) {
                    HashSet<String> list = emitted.get(terms.get(i));
                    if (list != null && list.contains(terms.get(j))) {
                    } else {
                        for (Url v : c.getUrls()) {
                            if (v.getDomain() != u.getDomain() && v.getFeatures().contains(terms.get(i)) && v.getFeatures().contains(terms.get(j))) {
                                break;
                            }
                            unsupported++;
                        }
                    }
                }
            }
        }
        return unsupported;
    }

    public boolean containsQuery(Url u, HashSet<String> query) {
        for (String term : query) {
            if (u.getFeatures().contains(term)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsEntireQuery(Url u, HashSet<String> query) {
        for (String term : query) {
            if (!u.getFeatures().contains(term)) {
                return false;
            }
        }
        return true;
    }
    
    public boolean validadatesEntireQuery(Cluster<UrlD> cluster, Url u, HashSet<String> query) {
        if (containsEntireQuery(u, query))
            return true;
        for (Edge e : new EdgeIterator(u)) {
            if (e.getUrl() != null && containsEntireQuery(e.getUrl(), query)) {
                return true;
            }
        }
        for (UrlD url : cluster.getUrls()) {
            if (containsEntireQuery(url, query) && url.linkedTo(u)) {
                return true;
            }
        }
        return false;
    }
    
    public boolean qualifies(Cluster<UrlD> c, HashSet<String> query) {
        for (UrlD url : c.getUrls()) {
            if (containsEntireQuery(url, query))
                return true;
        }
        return false;
    }

    public String cleanedTitle(ArrayList<String> tokenize) {
        if (tokenize.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String t : tokenize) {
            if (t.length() > 0) {
                sb.append(' ').append(t);
            }
        }
        return sb.deleteCharAt(0).toString();
    }

    public void updateEmittedDocs() {
            for (int i = 0; i < emitteddoc.size(); i++) {
                Map.Entry<Double, UrlD> entry = emitteddoc.remove(i);
                double d = this.termsrelevant.cosSim(entry.getValue().getFeatures());
                Map.Entry<Double, UrlD> createEntry = ArrayMap.createEntry(d, entry.getValue());
                emitteddoc.add(i, createEntry);
            }
            emitteddoc.descending();
    }

//    public void updateEmittedDocsAll() {
//            for (int i = 0; i < emitteddoc.size(); i++) {
//                Map.Entry<Double, Collection<String>> entry = emitteddoc.remove(i);
//                double d = this.termsallrelevant.cosSim(entry.getValue());
//                Map.Entry<Double, Collection<String>> createEntry = ArrayMap.createEntry(d, entry.getValue());
//                emitteddoc.add(i, createEntry);
//            }
//            emitteddoc.descending();
//    }

    public double score(Url u) {
        double score = 0;
        int count = 0;
        for (Url v : ((Cluster<UrlD>) u.getCluster()).getBase()) {
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
        for (UrlD base : c.getBase()) {
            if (!docadded.contains(base.getID())) {
                docadded.add(base.getID());
                //ArrayList<String> allterms = tokenizer.tokenize(base.getTitle());
                //if (true || containsQueryTerm(query, base)) {
                    this.termsrelevant.add(base.getFeatures());
                    //this.termsallrelevant.add(base.getFeatures());
                    this.emitteddoctime.add(base.getCreationTime(), base.getFeatures());
                //}
            }
        }
    }

//    public void addNonRel(HashSet<String> query, Cluster<UrlD> c) {
//        for (Url base : c.getUrls()) {
//            if (!nonreldocadded.contains(base.getID())) {
//                nonreldocadded.add(base.getID());
//                if (!containsQueryTerm(query, (UrlD) base) && this.score(base) < 0.5) {
//                    ArrayList<String> allterms = tokenizer.tokenize(base.getTitle());
//                    double sim = termsrelevant.cosSim(allterms, query);
//                    double nonsim = termsallrelevant.cosSim(allterms, query);
//                    if (nonsim > sim) {
//                        this.termsallrelevant.add(base.getFeatures());
//                    }
//                }
//            }
//        }
//    }

    public void addUrlToRelevanceModel(HashSet<String> query, Cluster<UrlD> c, UrlD u) {
        if (!docadded.contains(u.getID()) && c.getBase().contains(u)) {
            docadded.add(u.getID());
            if (containsQueryTerm(query, (UrlD) u)) {
                this.termsrelevant.add(u.getFeatures());
                this.emitteddoctime.add(u.getCreationTime(), u.getFeatures());
            }
        }
    }

    public void addBaseToRelevanceModel(HashSet<String> query, Cluster<UrlD> c, UrlD u) {
        for (Url base : c.getBase()) {
            if (!docadded.contains(base.getID()) && base != u) {
                docadded.add(base.getID());
                if (containsQueryTerm(query, (UrlD) base)) {
                    this.termsrelevant.add(base.getFeatures());
                    this.emitteddoctime.add(base.getCreationTime(), base.getFeatures());
                }
            }
        }
    }

    public void removeDoc(long timestamp) {
        timestamp -= forget;
        while (emitteddoctime.size() > 0) {
            Map.Entry<Long, HashSet<String>> entry = emitteddoctime.get(0);
            //log.info("remove %d %d", entry.getKey(), timestamp);
            if (entry.getKey() < timestamp) {
                //log.info("REMOVE %s", entry.getValue());
                termsrelevant.remove(entry.getValue());
                emitteddoctime.remove(0);
            } else {
                break;
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

    public Cluster createCluster(StreamClusterWritable cluster) {
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

    public static HashMap<Integer, TopicWritable> getTopics(String topicfile) {
        Datafile df = new Datafile(topicfile);
        TopicFile tf = new TopicFile(df);
        return tf.getMap();
    }

    class Model extends HashMapInt<String> {

        double termsseen = 0;
        double magnitude = 0;

        public void add(Collection<String> terms) {
            for (String term : terms) {
                add(term, 1);
            }
            termsseen += terms.size();
            magnitude = 0;
        }

        public void remove(Collection<String> terms) {
            for (String term : terms) {
                int freq = get(term);
                if (freq == 1) {
                    remove(term);
                } else {
                    put(term, freq - 1);
                }
            }
            termsseen -= terms.size();
            magnitude = 0;
        }

        public boolean hasChanged() {
            return magnitude == 0;
        }

        public double magnitude() {
            if (magnitude == 0 && termsseen > 0) {
                for (int i : values()) {
                    magnitude += i * i;
                }
                magnitude = Math.sqrt(magnitude);
            }
            return magnitude;
        }

        public double cosSim(Collection<String> titletokens) {
            double count = 0;
            double magnitude = 0;
            for (String term : titletokens) {
                Integer freq = get(term);
                if (freq != null) {
                    count += freq;
                    magnitude += freq * freq;
                }
            }
            if (magnitude == 0) {
                return 0;
            }
            double modelmagnitude = magnitude();
            if (modelmagnitude == 0) {
                return 0;
            }
            //log.info("cossim %f %f %f", count, magnitude, magnitude());
            return count / (Math.sqrt(magnitude) * modelmagnitude);
        }

        public double cosSim(Collection<String> titletokens, Collection<String> queryterms) {
            double count = 0;
            double magnitude = 0;
            for (String term : titletokens) {
                if (queryterms.contains(term)) {
                    Integer freq = get(term);
                    if (freq != null) {
                        count += freq;
                        magnitude += freq * freq;
                    }
                }
            }
            if (magnitude == 0) {
                return 0;
            }
            for (String term : queryterms) {

            }
            //log.info("cossim %f %f %f", count, magnitude, magnitude());
            return count / (Math.sqrt(magnitude) * magnitude());
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

        public double p(String term) {
            Integer freq = get(term);
            if (freq != null) {
                return freq / termsseen;
            }
            return 0;
        }

        public HashSet<String> highTerms(double thres) {
            int t = (int) Math.round(thres * termsseen);
            HashSet<String> terms = new HashSet();
            for (Map.Entry<String, Integer> term : this.entrySet()) {
                if (term.getValue() >= t) {
                    terms.add(term.getKey());
                }
            }
            return terms;
        }

        public boolean intop10(Collection<String> terms) {
            TopK<Integer> top10 = new TopK(10);
            top10.addAll(this.values());
            Integer lowest = top10.peek();
            log.info("LOWEST %d", lowest);
            for (String term : terms) {
                if (this.get(term) >= lowest) {
                    return true;
                }
            }
            return false;
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
}
