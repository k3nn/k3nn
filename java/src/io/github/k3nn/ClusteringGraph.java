package io.github.k3nn;

import io.hithub.k3nn.impl.NodeStoreIIBinary;
import io.hithub.k3nn.impl.NodeM;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.modules.RemoveFilteredWords;
import io.github.htools.extract.modules.StemTokens;
import io.github.htools.extract.modules.TokenToLowercase;
import io.github.htools.lib.Log;
import io.github.htools.lib.Profiler;
import io.github.htools.words.StopWordsMultiLang;
import io.github.htools.extract.DefaultTokenizerCased;
import io.github.htools.extract.modules.LStemTokens;
import io.github.htools.extract.modules.LowercaseTokens;
import io.github.htools.fcollection.FHashMapLongObject;
import io.github.htools.fcollection.FHashSet;
import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
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
public class ClusteringGraph<N extends Node> {

    public static final Log log = new Log(ClusteringGraph.class);
    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    public HashMap<Integer, Cluster<N>> clusters = new HashMap();
    //protected HashSet<Integer> changedclusters = new HashSet();
    public NodeStore<N> iinodes;
    protected int nextclusterid = 0;
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

    public ClusteringGraph() {
        iinodes = createII();
    }

    public void setNextClusterID(int id) {
        nextclusterid = id;
    }

    public NodeStore createII() {
        return new NodeStoreIIBinary();
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

    public static DefaultTokenizer getLancasterStemmedTokenizer() {
        HashSet<String> stemmedFilterSet = StopWordsMultiLang.get().getLancasterStemmedFilterSet();
        DefaultTokenizer t = new DefaultTokenizer();
        try {
            t.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        t.addEndPipeline(LStemTokens.class);
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

    public Cluster<N> createCluster(Collection<N> core) {
        Cluster<N> c = Cluster.createCoreCluster(this, nextclusterid++, core);
        clusters.put(c.getID(), c);
        return c;
    }

    public Cluster<N> createCluster(int id, Collection<N> base) {
        Cluster<N> c = Cluster.createCoreCluster(this, id, base);
        if (id == nextclusterid) {
            nextclusterid++;
        }
        clusters.put(c.getID(), c);
        return c;
    }

    public Cluster createCluster(int id) {
        Cluster c = new Cluster(this, id);
        nextclusterid = Math.max(nextclusterid, id + 1);
        clusters.put(c.getID(), c);
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
        clusters.remove(c.getID());
    }

    public void dispose() {
        clusters = null;
        iinodes = null;
    }

    public void addSentence(N sentence, HashSet<String> terms, boolean isCandidate) {
        add(sentence, terms);
        if (sentence.isClustered() && isCandidate) {
            clusteredCandidateSentence(sentence);
        }
    }

    public void clusteredCandidateSentence(N candidateSentence) {
    }

    public void add(N newNode, Collection<String> features) {
        log.info("add %d %s", newNode.getID(), newNode.getContent());

        //NodeS nodes = new NodeS(node.getID(), node.domain, "", features, node.creationtime);
        ArrayMapDouble<N> score2Node = iinodes.addGetList(newNode, features);
        iinodes.getNodes().put(newNode.getID(), newNode);

        // note: for trec we used > node.getLowestScore(), for ICTIR >=
        Profiler.startTime("updateNNLinks");
        FHashSet<Cluster> recheckClusters = new FHashSet();
        FHashSet<Cluster> updatedClusters = new FHashSet();
        for (Double2ObjectMap.Entry<N> entry : score2Node) {
            if (entry.getDoubleKey() > newNode.getLowestScore()) {
                newNode.add(new Edge(entry.getValue(), entry.getDoubleKey()));
            }
            if (entry.getDoubleKey() > entry.getValue().getLowestScore()) {
                N changedNode = entry.getValue();
                entry.getValue().add(new Edge(newNode, entry.getDoubleKey()));
                if (changedNode.isClustered()) {
                    Cluster cluster = changedNode.getCluster();
                    if (cluster.getCore().contains(changedNode)) {
                        recheckClusters.add(cluster);
                    } else if (changedNode.isClustered()) {
                        updatedClusters.add(changedNode.getCluster());
                    }
                }
            }
        }
        Profiler.addTime("updateNNLinks");

        Profiler.startTime("checkClusters");
        for (Cluster c : recheckClusters) {
            recheck(updatedClusters, c);
        }
        Profiler.addTime("checkClusters");

        Profiler.startTime("newcluster");
        Cluster newcluster = attemptNewCluster(updatedClusters, newNode);
        if (newcluster != null) {
            updatedClusters.add(newcluster);
        } else {
            newNode.setCluster(newNode.majority());
            if (newNode.isClustered()) {
                updatedClusters.add(newNode.getCluster());
            }
        }
        Profiler.addTime("newcluster");

        if (updatedClusters.size() > 0) {
            FHashSet<Cluster> alreadyUpdated = new FHashSet();
            FHashSet<Cluster> recheckClusters2 = new FHashSet();
            FHashSet<Cluster> updateClusters2 = new FHashSet();
            while (updatedClusters.size() > 0) {
                for (Cluster c : updatedClusters) {
                    if (clusters.containsKey(c.id)) {
                        assignNodes(recheckClusters2, updateClusters2, c);
                    }
                }
                recheckClusters2.removeAll(recheckClusters);
                for (Cluster c : recheckClusters2) {
                    recheck(updateClusters2, c);
                }
                if (updateClusters2.size() > 0) {
                    alreadyUpdated.addAll(updatedClusters);
                    updateClusters2.removeAll(alreadyUpdated);
                    updatedClusters = updateClusters2;
                    if (updatedClusters.size() > 0) {
                        recheckClusters.addAll(recheckClusters2);
                        recheckClusters2 = new FHashSet();
                        updateClusters2 = new FHashSet();
                    }
                } else {
                    updatedClusters = updateClusters2;
                }
            }
        }
    }

    public void recheck(FHashSet<Cluster> updatedClusters, Cluster c) {
        FHashSet<Node> core = findCore(c);
        if (core.isEmpty()) {
            ArrayList<Node> nodes = new ArrayList(c.getNodes());
            for (Node node : nodes) {
                node.setCluster(null);
            }
            remove(c);
        } else if (!c.getCore().equals(core)) {
            c.setCore(core);
            for (Node node : new ArrayList<Node>(c.getNodes())) {
                if (!core.contains(node)) {
                    node.setCluster(null);
                } else {
                    node.setCluster(c);
                }
            }
            updatedClusters.add(c);
        }
        if (c.watch) {
            c.evalall();
        }
    }

    private static FHashSet<Node> emptyList = new FHashSet<Node>();

    public static FHashSet<Node> findCore(Cluster c) {
        Profiler.startTime("findCore");
        for (Node u : (ArrayList<Node>)c.getNodes()) {
            if (u.possibleCoreNode()) {
                FHashSet<Node> base = u.get2DegenerateCore();
                if (base.size() > Cluster.BREAKTIE) {
                    Profiler.addTime("findCore");
                    return base;
                }
            }
        }
        Profiler.addTime("findCore");
        return emptyList;
    }

    
    
    public static void assignNodes(FHashSet<Cluster> recheckClusters, FHashSet<Cluster> updateClusters, Cluster<? extends Node> c) {
        Profiler.startTime("assignNodes2");
        FHashSet<? extends Node> nodes = c.findMajorityMembers();
        FHashSet<Node> existing = new FHashSet<Node>(c.getNodes());
        existing.removeAll(nodes);
        for (Node node : existing) {
            node.setCluster(null);
        }
        for (Node node : nodes) {
            if (node.isClustered() && node.getCluster() != c) {
                if (node.getCluster().getCore().contains(node)) {
                    recheckClusters.add(node.getCluster());
                } else {
                    updateClusters.add(node.getCluster());
                }
            }
            node.setCluster(c);
        }
        if (c.watch) {
            log.info("%s", c.evalall());
        }
        Profiler.addTime("assignNodes2");
    }

    public Cluster attemptNewCluster(FHashSet<Cluster> updatedClusters, N start) {
        Profiler.startTime("attemptNewCluster");
        if (start.possibleCoreNode()) {
            FHashSet<N> core = (FHashSet<N>) start.get2DegenerateCore();
            if (core.size() > Cluster.BREAKTIE) {
                for (N node : core) {
                    if (node.isClustered() && node.getCluster().getCore().contains(node)) {
                        Profiler.addTime("attemptNewCluster");
                        return null;
                    }
                }
                for (N node : core) {
                    if (node.isClustered()) {
                        updatedClusters.add(node.getCluster());
                    }
                }
                Cluster<N> newcluster = createCluster(core);
                updatedClusters.add(newcluster);
                Profiler.addTime("attemptNewCluster");
                return newcluster;
            }
        }
        Profiler.addTime("attemptNewCluster");
        return null;
    }

    public int unclustered() {
        int count = 0;
        for (N u : iinodes.getNodes().values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }

    public Edge createEdge(N owner, N destination, double score) {
        return new Edge(destination, score);
    }

    public long getExpirationTime(long creationtime) {
        return creationtime - 4 * 24 * 60 * 60;
    }

    public void purge(long creationtime) {
        Profiler.startTime("purge");
        long expirationTime = getExpirationTime(creationtime);
        this.iinodes.purge(expirationTime);
        purgeClusters(expirationTime);
        Profiler.addTime("purge");
    }

    public void purgeClusters(long expirationTime) {
        Iterator<Cluster<N>> iter = clusters.values().iterator();
        LOOP:
        while (iter.hasNext()) {
            Cluster<N> c = iter.next();
            for (N u : c.getNodes()) {
                if (u.getCreationTime() >= expirationTime) {
                    continue LOOP;
                }
            }
            iter.remove();
            for (N n : c.getNodes()) {
                getNodes().remove(n.getID());
            }
        }

    }

    public FHashMapLongObject<N> getNodes() {
        return iinodes.getNodes();
    }

    public N getNode(long id) {
        return iinodes.getNodes().get(id);
    }

    public void eval() {
        int sec = (int) (System.currentTimeMillis() - starttime) / 1000;
        log.info("urls %d", iinodes.getNodes().size());
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
