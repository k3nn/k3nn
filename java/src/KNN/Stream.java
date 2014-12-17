package KNN;

import io.github.repir.tools.Collection.ArrayMap;
import io.github.repir.tools.Collection.HashMapInt;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Extractor.DefaultTokenizer;
import io.github.repir.tools.Extractor.Tools.RemoveFilteredWords;
import io.github.repir.tools.Extractor.Tools.StemTokens;
import io.github.repir.tools.Extractor.Tools.TokenToLowercase;
import io.github.repir.tools.Lib.DateTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Profiler;
import io.github.repir.tools.Type.Tuple2;
import io.github.repir.tools.Type.Tuple2Comparable;
import io.github.repir.tools.Words.StopWordsMultiLang;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class Stream {

    public static final Log log = new Log(Stream.class);
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    public UrlMap urls = new UrlMap();
    public IIUrls iiurls = new IIUrls();
    static UrlM watch;
    long starttime = System.currentTimeMillis();

    static enum PROFILE {

        getBest,
        Majority,
        Majority2,
        addMajority,
        getBase,
        remove,
        linkedTo,
        getFree
    }

    public static DefaultTokenizer getStemmedTokenizer() {
        HashSet<String> stemmedFilterSet = StopWordsMultiLang.get().getStemmedFilterSet();
        DefaultTokenizer t = new DefaultTokenizer();
        try {
            t.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        t.addEndPipeline(StemTokens.class);
        t.addEndPipeline(new RemoveFilteredWords(t, stemmedFilterSet));
        return t;
    }

    public static DefaultTokenizer getUnstemmedTokenizer() {
        HashSet<String> unstemmedFilterSet = StopWordsMultiLang.get().getUnstemmedFilterSet();
        DefaultTokenizer t = new DefaultTokenizer();
        try {
            t.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        //t.addEndPipeline(StemTokens.class);
        t.addEndPipeline(new RemoveFilteredWords(t, unstemmedFilterSet));
        return t;
    }

    public void readFile(SentenceFile file) {
        file.setBufferSize(1000000);
        file.openRead();
        int perc = 0;
        for (SentenceWritable uw : file) {
            if (file.getOffset() * 100 / file.getLength() > perc) {
                log.info("Read %d%% %s", perc++, DateTools.toString(System.currentTimeMillis() / 1000));
            }
            UrlM url = (UrlM) urls.get(uw.id);
            if (url == null) {
                ArrayList<String> features = tokenizer.tokenize(uw.sentence);
                if (features.size() > 1) {
                    url = new UrlM(uw.id, uw.domain, uw.creationtime, features);
                    urls.put(uw.id, url);
                    add(url, features);
//                    if (urls.size() > 100000) {
//                        for (int i = 0; i < Cluster.idd; i++) {
//                            if (Cluster.clusters.containsKey(i)) {
//                                this.eval();
//                            }
//                        }
//                    }
                }
            }
        }
        file.closeRead();
    }

    public void add(Url url, Collection<String> features) {
//        if (url.url.contains("thirstythirty.blogspot.com")) {
//            watch = url;
//            log.setLevel(LEVEL.TRACE);
//        }
        //log.info("add %d", url.getID());
        ArrayMap<Tuple2Comparable<Integer, Long>, Url> map = iiurls.candidateUrlsCount(features);
        ArrayList<Url> changedUrls = new ArrayList();
        HashSet<Cluster> recheckcluster = new HashSet();
        double sqrtsize = Math.sqrt(((UrlM) url).countFeatures());
        for (Map.Entry<Tuple2Comparable<Integer, Long>, Url> entry : map.descending()) {
            Url u = entry.getValue();
            if (u.getDomain() != url.getDomain()) {
                double score = Score.timeliness(url.getCreationTime(), u.getCreationTime());
                //log.trace("timeliness %f %d %d", score, u.countFeatures(), url.countFeatures());
                if (score > 0) {
                    score *= entry.getKey().value1 / (sqrtsize * Math.sqrt(((UrlM) u).countFeatures()));
                    getBest3(url, u, score, changedUrls, recheckcluster);
                    if (score > 1) 
                       log.info("score > 1 time %f %d %f %f", 
                               Score.timeliness(url.getCreationTime(), u.getCreationTime()),
                               entry.getKey().value1, 
                               sqrtsize, 
                               Math.sqrt(((UrlM) u).countFeatures()));
                }
            }
        }
        //log.info("url %d cluster %d", url.getID(), url.isClustered() ? url.getCluster().getID() : -1);
        //log.info("changedUrls %d", changedUrls.size());
        if (changedUrls.size() > 0) {
            for (Cluster c : recheckcluster) {
                HashSet<Url> base = Cluster.getBase(c.getUrls());
                //if (changedUrls.contains(watch)) {
                //log.info("base %b %s", base.isEmpty(), base);
                //}
                if (base.isEmpty()) {
                    if (changedUrls.contains(watch)) {
                        log.info("base2 %b %s", base.isEmpty(), base);
                    }
                    for (Url u : c.getUrls()) {
                        u.setCluster(null);
                    }
                    Cluster.clusters.remove(c.id);
                    //if (c.id == Cluster.watch)
                    //log.info("remove rechecked cluster %d", c.id);
                } else {
                    changedUrls.removeAll(base);
                    //if (base.contains(watch)) {
                    //log.info("changed %b", changedUrls);
                    //}
                    if (!base.equals(c.getBase())) {
                        c.setBase(base);
                        for (Url u : c.getUrls()) {
                            if (!base.contains(u)) {
                                changedUrls.add(u);
                            }
                        }
                    }
                    //if (c.id == Cluster.watch)
                    //log.info("remove rechecked cluster %d base %s", c.id, base);

//                    if (base.contains(watch)) {
//                        log.info("changed %b", changedUrls);
//                    }
                }
            }
            resolve(url, changedUrls);
        } else {
            url.setCluster(url.majority());
        }
        iiurls.add(url, features);
    }

    public void resolve(Url url, ArrayList<Url> changed) {
        //log.info("resolve %d", url.getID());
        HashSet<Url> changed2 = new HashSet();
        for (Url u : changed) {
            if (!changed2.contains(u)) {
                if (u == watch) {
                    log.info("resolve", u.toStringEdges());
                }
                if (u.isClustered()) {
                    //log.info("resolve %d was in cluster %d", u.getID(), u.getCluster().getID());
                    u.getCluster().remove(u, changed2);
                } else {
                    //log.info("resolve %d unclustered", u.getID());
                    changed2.add(u);
                }
            }
        }
        if (changed2.size() > 0) {
            Cluster.addMajority(changed2);
        }
        url.setCluster(url.majority());
        if (url == watch) {
            log.trace("resolve url.majority %s", url.getCluster());
        }
        if (url.isClustered()) {
            //log.info("resolve assign to majority %s\n%s", url.url, url.cluster);
            reinsert(changed2);
        }

        if (!url.isClustered()) {
            url.clusterMajorityBase();
        }
    }

    public void reinsert(HashSet<Url> reinsert) {
        for (Url u : reinsert) {
            u.setCluster(u.majority());
        }
    }

    public void getBest3(Url url,
            Url u,
            double score,
            ArrayList<Url> changes,
            HashSet<Cluster> recheckcluster) {
        Profiler.startTime(PROFILE.getBest.name());
        //log.info("getBest1 url %s cluster %s", url.url, u);
        if (score > url.getLowestScore()) {
            Edge e = new Edge(u, score);
            url.add(e);
        }
        if (!(u instanceof UrlF) && score > u.getLowestScore()) {
            Edge e = new Edge(url, score);
            if (!u.isClustered() || (u.getEdges() == Cluster.K && u.edge[Cluster.K - 1].url.getCluster() == u.getCluster())) {
                if (u == watch) {
                    log.trace("getBest3 %b %b", u.isClustered(), u.getCluster().getBase().contains(u));
                }
                if (u.isClustered() && u.getCluster().getBase().contains(u)) {
                    recheckcluster.add(u.getCluster());
                }
                changes.add(u);
            }
            u.add(e);
            if (u == watch) {
                log.trace("steal %s %s", u.toString(), e);
                log.trace("%s", u.toStringEdges());
            }
        }
        Profiler.addTime(PROFILE.getBest.name());
    }

    public int unclustered() {
        int count = 0;
        for (UrlM u : urls.values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }

    public void eval() {
        int sec = (int) (System.currentTimeMillis() - starttime) / 1000;
        log.info("urls %d", this.urls.size());
        log.info("clusters %d", Cluster.clusters.size());
        log.info("unclusterd %d", unclustered());
        log.printf("time past %d:%ds", sec / 60, sec % 60);
        HashMapInt<Integer> dist = new HashMapInt();
        for (Cluster cl : Cluster.clusters.values()) {
            dist.add(cl.size(), 1);
        }
        TreeMap<Integer, Integer> sorted = new TreeMap(dist);
        log.info("sorted %s", sorted);

        Profiler.reportProfile();
        log.exit();
    }

    public static void main(String[] args) {
        Stream s = new Stream();
        SentenceFile uf = new SentenceFile(new Datafile(args[0]));
        s.readFile(uf);
        s.eval();
    }
}
