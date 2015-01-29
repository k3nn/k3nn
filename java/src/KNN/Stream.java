package KNN;

import static KNN.Url.listenclusters;
import static KNN.Url.listenurls;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.extract.modules.RemoveFilteredWords;
import io.github.repir.tools.extract.modules.StemTokens;
import io.github.repir.tools.extract.modules.TokenToLowercase;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.Profiler;
import io.github.repir.tools.type.Tuple2Comparable;
import io.github.repir.tools.Words.StopWordsMultiLang;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author jeroen
 */
public class Stream<U extends Url> {

    public static final Log log = new Log(Stream.class);
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    public HashMap<Integer, Cluster<U>> clusters = new HashMap();
    protected HashSet<Integer> changedclusters = new HashSet();
    protected UrlClusterListener listener;
    public UrlMap<U> urls = new UrlMap();
    public IIUrls<U> iiurls = new IIUrls();
    private int nextclusterid = 0;
    protected static UrlM watch;
    protected long starttime = System.currentTimeMillis();

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

    public void setNextClusterID(int id) {
        nextclusterid = id;
    }

    public void setListenr(UrlMap<U> map, Collection<Integer> list, UrlClusterListener listener) {
        listenclusters = new HashSet();
        listenurls = new HashSet(list);
        this.listener = listener;
        for (int id : list) {
            U url = map.get(id);
            if (url != null && url.isClustered()) {
                listenclusters.add(url.getCluster().getID());
            }
        }
    }

    public void setListenAll(UrlClusterListener listener) {
        this.listener = listener;
        listenclusters = Url.allclusters;
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

    public static ArrayList<String> getFeatures(String title) {
        return tokenizer.tokenize(title);
    }

    public Cluster<U> createCluster(Collection<U> base) {
        Cluster<U> c = new Cluster(this, nextclusterid++, base);
        clusters.put(c.getID(), c);
        changedclusters.add(c.getID());
        return c;
    }

    public Cluster<U> createCluster(int id, Collection<U> base) {
        Cluster<U> c = new Cluster(this, id, base);
        if (id == nextclusterid)
            nextclusterid++;
        clusters.put(c.getID(), c);
        changedclusters.add(c.getID());
        return c;
    }

    public Cluster createCluster(int id) {
        Cluster c = new Cluster(this, id);
        nextclusterid = Math.max(nextclusterid, id + 1);
        clusters.put(c.getID(), c);
        changedclusters.add(id);
        return c;
    }

    public Collection<Cluster<U>> getClusters() {
        return clusters.values();
    }

    public void setStartClusterID(int nextclusterid) {
        this.nextclusterid = nextclusterid;
    }

    public Cluster getCluster(int id) {
        return clusters.get(id);
    }

    public void remove(Cluster c) {
        if (c.watch) {
            log.info("remove cluser %d", c.getID());
        }
        changedclusters.add(c.getID());
        clusters.remove(c.getID());
//        if (nextclusterid == c.getID() + 1) {
//            nextclusterid--;
//        }
    }

    public void dispose() {
        clusters = null;
        iiurls = null;
        urls = null;
    }

    public void resetChangedCluster() {
        changedclusters = new HashSet();
    }

    public HashSet<Integer> getChangedCluster() {
        return changedclusters;
    }

    public void add(U url, Collection<String> features) {
        //log.info("add %d", url.getID());
        if (Url.modifiedurls.size() > 0) {
            Url.modifiedurls = new HashSet();
        }
        ArrayMap<Tuple2Comparable<Integer, Integer>, U> map = iiurls.candidateUrlsCount(features);
        HashSet<Url> changedUrls = new HashSet();
        HashSet<Cluster<U>> recheckcluster = new HashSet();
        double sqrtsize = Math.sqrt(((UrlM) url).countFeatures());
        double lowestscore = 0;

        for (Map.Entry<Tuple2Comparable<Integer, Integer>, U> entry : map.descending()) {
            U u = entry.getValue();
            if (u.getDomain() != url.getDomain()) {
                double score = Score.timeliness(url.getCreationTime(), u.getCreationTime());
                //log.trace("timeliness %f %d %d", score, u.countFeatures(), url.countFeatures());
                if (score > lowestscore || score > u.getLowestScore()) {
                    score *= entry.getKey().value1 / (sqrtsize * Math.sqrt(((UrlM) u).countFeatures()));
                    if (score > lowestscore || score > u.getLowestScore()) {
                        getBest3(url, u, score, changedUrls, recheckcluster);
                        lowestscore = url.getLowestScore();
                    }
                }
            }
        }
        //log.info("url %d cluster %d", url.getID(), url.isClustered() ? url.getCluster().getID() : -1);
        //log.info("changedUrls %d", changedUrls.size());
        if (changedUrls.size() > 0) {
            for (Cluster<U> c : recheckcluster) {
                HashSet<Url> base = Cluster.getBase(c.getUrls());
                if (c.watch) {
                    log.info("recheckcluster %d baseempty %b", c.getID(), base.isEmpty());
                }
                //if (changedUrls.contains(watch)) {
                //log.info("base %b %s", base.isEmpty(), base);
                //}
                if (base.isEmpty()) {
                    if (changedUrls.contains(watch)) {
                        log.info("base2 %b %s", base.isEmpty(), base);
                    }
                    ArrayList<U> urls = new ArrayList(c.getUrls());
                    for (U u : urls) {
                        u.setCluster(null);
                        changedUrls.add(u);
                    }
                    remove(c);
                    if (c.watch) {
                        log.info("remove rechecked cluster %d", c.id);
                    }
                } else {
                    changedUrls.removeAll(base);
                    if (c.watch) {
                        log.info("changed %b", changedUrls);
                    }
                    if (!base.equals(c.getBase())) {
                        c.setBase(base);
                        //findPossibleMembers(changedUrls, c, c.getBase());
                    }
                    if (c.watch) {
                        log.info("remove rechecked cluster %d base %s", c.id, base);
                    }

                    if (c.watch) {
                        log.info("changed %s", changedUrls);
                    }
                }
            }
            resolve(url, changedUrls);
        } else {
            url.setCluster(url.majority());
        }
        iiurls.add(url, features);
        if (Url.modifiedurls.size() > 0) {
            listener.urlChanged(url, Url.modifiedurls);
        }
    }

    public void findPossibleMembers(HashSet<Url> changedUrls, Cluster<U> cluster, HashSet<U> base) {
        for (U u : cluster.getUrls()) {
            if (!base.contains(u)) {
                changedUrls.add(u);
            }
        }
    }

    public void resolve(U url, HashSet<Url> changed) {
        //log.info("resolve %d", url.getID());
        HashSet<Url> changed2 = new HashSet();
        for (Url u : changed) {
            if (u.watch) {
                log.info("resolve", u.toClusterString());
            }
            if (u.isClustered()) {
                //log.info("resolve %d was in cluster %d", u.getID(), u.getCluster().getID());
                Cluster<U> c = u.getCluster();
                for (U n : new ArrayList<U>(c.getUrls())) {
                    if (!c.getBase().contains(n)) {
                        n.setCluster(null);
                        changed2.add(n);
                    }
                }
            } else 
                changed2.add(u);
            //}
        }
        if (changed.size() > 0) {
            addMajority(changed2);
        }
        url.setCluster(url.majority());
        if (url.watch) {
            log.trace("resolve url.majority %s", url.getCluster());
        }
        if (url.isClustered()) {
            //log.info("resolve assign to majority %s\n%s", url.getID(), url.getCluster().getID());
            reinsert(changed2);
        }

        if (!url.isClustered()) {
            url.clusterMajorityBase(this);
        }
    }

    public void addMajority(HashSet<? extends Url> urls) {
        int size = 0;
        while (size != urls.size()) {
            size = urls.size();
            HashSet<Url> newset = new HashSet();
            for (Url u : urls) {
                Cluster c = u.majority();
                if (u.watch)
                    log.info("addMajority %d %s %s", u.getID(), u.getCluster(), c);
                if (c != null && c != u.getCluster()) {
                    //log.info("majority %d %d", u.getID(), c.getID());
                    u.setCluster(c);
                } else {
                    newset.add(u);
                }
            }
            urls = newset;
        }
    }
    
    
    public void reinsert(HashSet<Url> reinsert) {
        for (Url u : reinsert) {
            u.setCluster(u.majority());
        }
    }

    public Edge createEdge(U owner, U destination, double score) {
        return new Edge(destination, score);
    }

    public void getBest3(U newurl,
            U existing,
            double score,
            HashSet<Url> changes,
            HashSet<Cluster<U>> recheckcluster) {
        //log.info("getBest1 url %s cluster %s", url.url, u);
        if (score > newurl.getLowestScore()) {
            newurl.add(createEdge(newurl, existing, score));
        }
        if (newurl.watch) {
            log.info("getBest3 url %d %s %s", newurl.getID(), newurl.getNN(), newurl.getScore());
        }
        if (score > existing.getLowestScore()) {
            Edge e = createEdge(existing, newurl, score);
            if (!existing.isClustered() || (existing.getEdges() == Cluster.K && existing.edge[Cluster.K - 1].url.getCluster() == existing.getCluster())) {
                if (existing.isClustered() && existing.getCluster().getBase().contains(existing)) {
                    Cluster c = existing.getCluster();
                    if (existing.getLowestNN().getCluster() == c) {
                        if (c.watch) {
                            log.info("steal cluster %d url %d nn %d score %f newnn %d newscore %f",
                                    c.getID(), existing.getID(), existing.getLowestNN().getID(), existing.getLowestScore(), newurl.getID(), score);
                        }
                        recheckcluster.add(c);
                    }
                }
                changes.add(existing);
            }
            if (existing.watch && existing.getEdges() == Cluster.K) {
                Url nn = existing.getLowestNN();
                //log.info("steal url %d nn %d score %f", existing.getID(), nn.getID(), existing.getLowestScore());
            }
            existing.add(e);
            if (existing.watch && existing.getEdges() == Cluster.K) {
                log.info("steal url %d %s %s", existing.getID(), existing.getNN(), existing.getScore());
            }
        }
    }

    public int unclustered() {
        int count = 0;
        for (U u : urls.values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }

    public void eval() {
        int sec = (int) (System.currentTimeMillis() - starttime) / 1000;
        log.info("urls %d", this.urls.size());
        log.info("clusters %d", getClusters().size());
        log.info("unclusterd %d", unclustered());
        log.printf("time past %d:%ds", sec / 60, sec % 60);
        HashMapInt<Integer> dist = new HashMapInt();
        for (Cluster cl : getClusters()) {
            dist.add(cl.size(), 1);
        }
        TreeMap<Integer, Integer> sorted = new TreeMap(dist);
        log.info("sorted %s", sorted);

        Profiler.reportProfile();
        log.exit();
    }
}
