package io.github.k3nn.impl;

import io.github.k3nn.Cluster;
import io.github.k3nn.NodeStore;
import io.github.k3nn.Score;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.collection.TopKMap;
import io.github.htools.fcollection.FHashMapIntInt;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashMapLongObject;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * An inverted index to retrieve nodes that contain a feature.
 *
 * @author jeroen
 */
public class NodeStoreIG<N extends NodeVector> implements NodeStore<N> {

    public static final Log log = new Log(NodeStoreIG.class);
    public FHashMapIntList<N> bands3 = new FHashMapIntList(5000000);
    public FHashMapIntList<N> bands2 = new FHashMapIntList(4000000);
    protected FHashMapLongObject<N> nodes = new FHashMapLongObject();

    /**
     * Add a node to the postings lists of the terms
     *
     * @param node
     * @param terms
     */
    public void add(N node, Collection<String> terms) {
        if (terms.size() > 1) {
            ArrayList<String> list = (ArrayList) terms;
            FHashSet<Integer> shash2 = new FHashSet();
            FHashSet<Integer> shash3 = new FHashSet();
            int hash0;
            int hash1 = list.get(0).hashCode();
            int hash2 = list.get(1).hashCode();
            shash2.add(MathTools.hashCode(hash1, hash2));
            for (int i = 2; i < list.size(); i++) {
                hash0 = hash1;
                hash1 = hash2;
                hash2 = list.get(i).hashCode();
                shash2.add(MathTools.hashCode(hash1, hash2));
                shash3.add(MathTools.hashCode(hash0, hash1, hash2));
            }
            for (Integer h : shash2) {
                bands2.add(h, node);
            }
            for (Integer h : shash3) {
                bands3.add(h, node);
            }
        }
    }

    public ArrayMapDouble<N> addGetList(N node, Collection<String> terms) {
        ArrayMapDouble<N> top = new ArrayMapDouble();
        //log.info("addGetList %d %d", node.getID(), terms.size());
        if (terms.size() > 1) {
            long expiration = node.getCreationTime() - 3 * 24 * 60 * 60;
            ArrayList<String> list = (ArrayList) terms;
            FHashMapIntInt shash2 = new FHashMapIntInt();
            FHashMapIntInt shash3 = new FHashMapIntInt();
            int hash0;
            int hash1 = list.get(0).hashCode();
            int hash2 = list.get(1).hashCode();
            shash2.add(MathTools.hashCode(hash1, hash2));
            for (int i = 2; i < list.size(); i++) {
                hash0 = hash1;
                hash1 = hash2;
                hash2 = list.get(i).hashCode();
                shash2.add(MathTools.hashCode(hash1, hash2));
                shash3.add(MathTools.hashCode(hash0, hash1, hash2));
            }
            FHashSet<N> candidates = new FHashSet();
            addCandidates3(node, candidates, shash3, expiration);
            if (candidates.size() < 10) {
                addCandidates2(node, candidates, shash2, expiration);
            }
            for (int h : shash2.keySet()) {
                bands2.add(h, node);
                //log.info("bands2 %d %s", h, bands2.get(h));
            }
            for (int h : shash3.keySet()) {
                bands3.add(h, node);
                //log.info("bands3 %d %s", h, bands3.get(h));
            }
            //log.info("addGetList %d %d", node.getID(), candidates.size());
            for (N candidate : candidates) {
                double score = score(node, candidate);
                if (score > 0) {
                    top.add(score, candidate);
                }
            }
        }
        //log.info("addGetList %d %s", node.getID(), top.size());
        return top;
    }

    public FHashSet<N>[] getNodes(N node, FHashMapIntInt shash, FHashMapIntList<N> hashtable, long expiration) {
        FHashSet<N>[] nodes = new FHashSet[2];
        for (int i = 0; i < 2; i++) {
            nodes[i] = new FHashSet();
        }
        for (Int2IntMap.Entry hash : shash.int2IntEntrySet()) {
            hash.setValue(hash.getIntValue() == 1 ? 0 : 1);
            ObjectArrayList<N> list = hashtable.get(hash.getIntKey());
            if (list != null) {
                int expiredindex = -1;
                for (int i = 0; i < list.size() && list.get(i).getCreationTime() <= expiration; i++) {
                    expiredindex = i;
                }
                if (expiredindex > -1) {
                    list.removeElements(0, expiredindex + 1);
                }
                // save time, n-grams that occur in more than 5% of the documents are slow and 
                // poor predictors of similarity
                if (list.size() < this.nodes.size() / 10 || list.size() < 100) {
                    for (N n : list) {
                        if (n.domain != node.domain) {
                            nodes[hash.getIntValue()].add(n);
                        }
                    }
                }
            }
        }
        return nodes;
    }

    public void addCandidates2(N node, FHashSet<N> candidates, FHashMapIntInt shash2, long expiration) {
        int countbefore = candidates.size();
        FHashSet<N>[] nodes = getNodes(node, shash2, bands2, expiration);
        candidates.addAll(nodes[1]);
        //log.info("candidates 2-grams >1 %d", candidates.size() - countbefore);
        if (candidates.size() < 10) {
            candidates.addAll(nodes[0]);
            //log.info("candidates 2-grams 1 %d", candidates.size() - countbefore);
        }
    }

    public void addCandidates3(N node, FHashSet<N> candidates, FHashMapIntInt shash3, long expiration) {
        int countbefore = candidates.size();
        FHashSet<N>[] nodes = getNodes(node, shash3, bands3, expiration);
        candidates.addAll(nodes[1]);
        //log.info("candidates 3-grams >1 %d", candidates.size() - countbefore);
        if (candidates.size() < 10) {
            candidates.addAll(nodes[0]);
            //log.info("candidates 3-grams 1 %d", candidates.size() - countbefore);
        }
    }

    public double score(N a, N b) {
        return (1 - a.getVector().ignorm(b.getVector())) * Score.timeliness(a.getCreationTime(), b.getCreationTime());
    }

    public void purge(long expirationDatetime) {
        ObjectIterator<Int2ObjectMap.Entry<ObjectArrayList<N>>> iter = bands2.int2ObjectEntrySet().iterator();
        while (iter.hasNext()) {
            Int2ObjectMap.Entry<ObjectArrayList<N>> next = iter.next();
            ObjectArrayList<N> value = next.getValue();
            if (value.size() == 0 || value.get(value.size() - 1).getCreationTime() < expirationDatetime) {
                iter.remove();
            }
        }
        ObjectIterator<Int2ObjectMap.Entry<ObjectArrayList<N>>> iter2 = bands3.int2ObjectEntrySet().iterator();
        while (iter2.hasNext()) {
            Int2ObjectMap.Entry<ObjectArrayList<N>> next = iter2.next();
            ObjectArrayList<N> value = next.getValue();
            if (value.size() == 0 || value.get(value.size() - 1).getCreationTime() < expirationDatetime) {
                iter.remove();
            }
        }
        Iterator<N> itern = getNodes().values().iterator();
        while (itern.hasNext()) {
            N n = itern.next();
            if (!n.isClustered() && n.getCreationTime() < expirationDatetime) {
                itern.remove();
            }
        }

    }

    @Override
    public FHashMapLongObject<N> getNodes() {
        return nodes;
    }
}
