package KNN;

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
import io.github.repir.tools.extract.DefaultTokenizerCased;
import io.github.repir.tools.extract.modules.ConvertToLowercase;
import io.github.repir.tools.extract.modules.LowercaseTokens;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author jeroen
 */
public class Stream<N extends Node> {

    public static final Log log = new Log(Stream.class);
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    public HashMap<Integer, Cluster<N>> clusters = new HashMap();
    //protected HashSet<Integer> changedclusters = new HashSet();
    public HashMap<Long, N> nodes = new HashMap();
    public IINodes iinodes;
    private int nextclusterid = 0;
    protected static NodeM watch;
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

    public Stream() {
        iinodes = createII();
    }

    public void setNextClusterID(int id) {
        nextclusterid = id;
    }

    public IINodes createII() {
        return new IINodesBinary();
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

    public static DefaultTokenizer getUnstemmedTokenizer2() {
        HashSet<String> unstemmedFilterSet = StopWordsMultiLang.get().getUnstemmedFilterSet();
        DefaultTokenizerCased t = new DefaultTokenizerCased();

        //t.addEndPipeline(StemTokens.class);
        t.addEndPipeline(new RemoveFilteredWords(t, unstemmedFilterSet));
        t.addEndPipeline(LowercaseTokens.class);
        return t;
    }

    public static ArrayList<String> getFeatures(String title) {
        return tokenizer.tokenize(title);
    }

    public Cluster<N> createCluster(Collection<? extends Node> base) {
        Cluster<N> c = new Cluster(this, nextclusterid++, base);
        clusters.put(c.getID(), c);
        //changedclusters.add(c.getID());
        return c;
    }

    public Cluster<N> createCluster(int id, Collection<N> base) {
        Cluster<N> c = new Cluster(this, id, base);
        if (id == nextclusterid) {
            nextclusterid++;
        }
        clusters.put(c.getID(), c);
        //changedclusters.add(c.getID());
        return c;
    }

    public Cluster createCluster(int id) {
        Cluster c = new Cluster(this, id);
        nextclusterid = Math.max(nextclusterid, id + 1);
        clusters.put(c.getID(), c);
        //.add(id);
        return c;
    }

    public Collection<Cluster<N>> getClusters() {
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
        clusters.remove(c.getID());
    }

    public void dispose() {
        clusters = null;
        iinodes = null;
        nodes = null;
    }

    public void add(N node, Collection<String> features) {
        NodeS nodes = new NodeS(node.getID(), node.domain, "", features, node.creationtime);
        ArrayMap<Double, N> map = iinodes.candidateUrlsCount(nodes).descending();
        HashSet<N> changedNodes = new HashSet();
        HashSet<Cluster> recheckcluster = new HashSet();

        for (int i = 0; i < map.size() && i < Cluster.K; i++) {
            node.add(new Edge(map.getValue(i), map.getKey(i)));
        }

        for (Map.Entry<Double, N> entry : map.descending()) {
            N nodeb = entry.getValue();
            double score = entry.getKey();
            if (score > nodeb.getLowestScore()) {
                nodeb.add(new Edge(node, score));
                changedNodes.add(nodeb);
            }
        }
        HashSet<N> changed2 = new HashSet();
        while (changedNodes.size() > 0) {
            HashSet<N> newchanged = new HashSet();
            for (N u : changedNodes) {
                if (u.isClustered()) {
                    Cluster<N> c = u.getCluster();
                    if (c.getBase().contains(u)) {
                        recheckcluster.add(c);
                    } else {
                        u.setCluster(null);
                        if (((Node) u).backlinks != null) {
                            for (Node b : ((Node) u).backlinks) {
                                if (b.isClustered() && b.getCluster() == c) {
                                    newchanged.add((N) b);
                                }
                            }
                        }
                        changed2.add(u);
                    }
                }
            }
            changedNodes = newchanged;
        }
        //log.info("changedUrls %d %s", changed2.size(), changed2);
        for (Cluster<N> c : recheckcluster) {
            HashSet<Node> base = findBase(c);
            //if (c.watch) {
            //log.info("recheckcluster %d baseempty %b", c.getID(), base.isEmpty());
            //}
            if (base.isEmpty()) {
                if (changed2.contains(watch)) {
                    log.info("base2 %b %s", base.isEmpty(), base);
                }
                ArrayList<N> urls = new ArrayList(c.getNodes());
                for (N u : urls) {
                    u.setCluster(null);
                }
                remove(c);
                if (c.watch) {
                    log.info("remove rechecked cluster %d", c.getID());
                }
            } else if (!c.getBase().equals(base)) {
                c.setBase(base);
                ArrayList<N> list = new ArrayList(c.getNodes());
                list.removeAll(base);
                for (Node u : list) {
                    u.setCluster(null);
                }
                this.assignNodes(c);
                changed2.removeAll(c.getNodes());
            }
        }
        if (!node.isClustered()) {
            changed2.add(node);
        }
        addMajority(changed2);
        if (!node.isClustered()) {
            clusterMajorityBase(node);
        } else if (node.backlinks != null) {
            assignNodes(node.getCluster());
        }
        iinodes.add(node, features);
    }

    public double score(N a, N b, int count, double sqrta) {
        if (a.domain == b.domain) {
            return 0;
        }
        return Score.timeliness(a.getCreationTime(), b.getCreationTime())
                * count / (sqrta * Math.sqrt(((NodeM) b).countFeatures()));
    }

    public HashSet<Node> findBase(Cluster<N> c) {
        for (N u : c.getBase()) {
            HashSet<Node> base = u.getBase();
            if (base.size() > Cluster.BREAKTIE) {
                return base;
            }
        }
        return new HashSet<Node>();
    }

    public void assignNodes(Cluster<N> c) {
        HashSet<N> newnodes = new HashSet(c.getNodes());
        HashMapInt<N> votes = new HashMapInt();
        while (newnodes.size() > 0) {
            for (Node u : newnodes) {
                if (u.backlinks != null) {
                    for (Node b : u.backlinks) {
                        if (!b.isClustered() && !c.getNodes().contains(b)) {
                            votes.add((N) b, 1);
                        }
                    }
                }
            }
            newnodes = new HashSet();
            Iterator<Map.Entry<N, Integer>> iter = votes.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<N, Integer> next = iter.next();
                N u = next.getKey();
                if (next.getValue() > Cluster.BREAKTIE) {
                    iter.remove();
                    if (!u.isClustered()) {
                        next.getKey().setCluster(c);
                        newnodes.add(next.getKey());
                    }
                }
            }
        }
    }

    public void clusterMajorityBase(N start) {
        HashSet<Node> base = start.getBase();
        if (base.size() > Cluster.BREAKTIE) {
            Cluster<N> newcluster = createCluster(base);
            assignNodes(newcluster);
        }
    }

    public int unclustered() {
        int count = 0;
        for (N u : nodes.values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }

    public void resolve(N url, HashSet<Node> changed) {
        //log.info("resolve %d", url.getID());
        HashSet<Node> changed2 = new HashSet();
        for (Node u : changed) {
            if (u.isClustered()) {
                //log.info("resolve %d was in cluster %d", u.getID(), u.getCluster().getID());
                Cluster<N> c = u.getCluster();
                for (N n : new ArrayList<N>(c.getNodes())) {
                    if (!c.getBase().contains(n)) {
                        n.setCluster(null);
                        changed2.add(n);
                    }
                }
            } else {
                changed2.add(u);
            }
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
            this.clusterMajorityBase(url);
        }
    }

    public void addMajority(HashSet<? extends KNN.Node> urls) {
        int size = 0;
        while (size != urls.size()) {
            size = urls.size();
            HashSet<KNN.Node> newset = new HashSet();
            for (KNN.Node u : urls) {
                Cluster c = u.majority();
                if (u.watch) {
                    log.info("addMajority %d %s %s", u.getID(), u.getCluster(), c);
                }
                if (c != null && c != u.getCluster()) {
                    //log.info("majority %d %d", u.getID(), c.getID());
                    u.setCluster(c);
                    if (((Node) u).backlinks != null) {
                        for (Node b : ((Node) u).backlinks) {
                            if (!b.isClustered()) {
                                newset.add(b);
                            }
                        }
                    }
                } else {
                    newset.add(u);
                }
            }
            urls = newset;
        }
    }

    public void reinsert(HashSet<Node> reinsert) {
        for (Node u : reinsert) {
            u.setCluster(u.majority());
        }
    }

    public Edge createEdge(N owner, N destination, double score) {
        return new Edge(destination, score);
    }

//    public void getBest3(N newnode,
//            N existingnode,
//            double score,
//            HashSet<Node> changes,
//            HashSet<Cluster<N>> recheckcluster) {
//        //log.info("getBest1 url %s cluster %s", url.url, u);
//        if (score > newnode.getLowestScore()) {
//            newnode.add(createEdge(newnode, existingnode, score));
//        }
//        if (newnode.watch) {
//            log.info("getBest3 url %d %s %s", newnode.getID(), newnode.getNN(), newnode.getScore());
//        }
//        if (score > existingnode.getLowestScore()) {
//            Edge e = createEdge(existingnode, newnode, score);
//            if (!existingnode.isClustered() || (existingnode.getEdges() == Cluster.K && existingnode.getNN(Cluster.K - 1).node.getCluster() == existingnode.getCluster())) {
//                if (existingnode.isClustered() && existingnode.getCluster().getBase().contains(existingnode)) {
//                    Cluster c = existingnode.getCluster();
//                    if (existingnode.getLowestNN().getCluster() == c) {
//                        if (c.watch) {
//                            log.info("steal cluster %d url %d nn %d score %f newnn %d newscore %f",
//                                    c.getID(), existingnode.getID(), existingnode.getLowestNN().getID(), existingnode.getLowestScore(), newnode.getID(), score);
//                        }
//                        recheckcluster.add(c);
//                    }
//                }
//                changes.add(existingnode);
//            }
//            if (existingnode.watch && existingnode.getEdges() == Cluster.K) {
//                Node nn = existingnode.getLowestNN();
//                //log.info("steal url %d nn %d score %f", existing.getID(), nn.getID(), existing.getLowestScore());
//            }
//            existingnode.add(e);
//            if (existingnode.watch && existingnode.getEdges() == Cluster.K) {
//                log.info("steal url %d %s %s", existingnode.getID(), existingnode.getNN(), existingnode.getScore());
//            }
//        }
//    }
    public void purge(long datetime) {
        this.iinodes.purge(datetime);
    }

    public void eval() {
        int sec = (int) (System.currentTimeMillis() - starttime) / 1000;
        log.info("urls %d", this.nodes.size());
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
