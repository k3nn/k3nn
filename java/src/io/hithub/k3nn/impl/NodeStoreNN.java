package io.hithub.k3nn.impl;

import io.github.k3nn.NodeStore;
import io.github.k3nn.Score;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.fcollection.FHashMapIntList;
import io.github.htools.fcollection.FHashMapList;
import io.github.htools.fcollection.FHashMapLongObject;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.fcollection.FHashSetInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * An inverted index to retrieve nodes that contain a feature.
 *
 * @author jeroen
 */
public class NodeStoreNN<N extends NodeVector> implements NodeStore<N> {

    public static final Log log = new Log(NodeStoreNN.class);
    public FHashMapList<String, N> termmap = new FHashMapList(4000000);
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
            int hash0;
            int hash1 = list.get(0).hashCode();
            for (int i = 1; i < list.size(); i++) {
                hash0 = hash1;
                hash1 = list.get(i).hashCode();
                shash2.add(MathTools.hashCode(hash0, hash1));
            }
            for (Integer h : shash2) {
                bands2.add(h, node);
            }
            for (String term : new HashSet<String>(terms)) {
                this.termmap.add(term, node);
            }
        }
    }

    public ArrayMapDouble<N> addGetList(N node, Collection<String> terms) {
        ArrayMapDouble<N> top = new ArrayMapDouble();
        //log.info("addGetList %d %d", node.getID(), terms.size());
        if (terms.size() > 1) {
            long expiration = node.getCreationTime() - 3 * 24 * 60 * 60;
            ArrayList<String> list = (ArrayList) terms;
            HashSet<String> nodeterms = new HashSet(terms);
            FHashSetInt shash2 = new FHashSetInt();
            int hash0;
            int hash1 = list.get(0).hashCode();
            for (int i = 2; i < list.size(); i++) {
                hash0 = hash1;
                hash1 = list.get(i).hashCode();
                shash2.add(MathTools.hashCode(hash0, hash1));
            }
            FHashSet<N> candidates = new FHashSet();
            addCandidates2(node, candidates, shash2, expiration);
            if (candidates.size() < 3) {
                addCandidatesTerms(node, candidates, nodeterms, expiration);
            }
            for (int h : shash2) {
                bands2.add(h, node);
                //log.info("bands2 %d %s", h, bands2.get(h));
            }
            for (String term : nodeterms) {
                this.termmap.add(term, node);
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

    public FHashSet<N> getNodes(N node, FHashSetInt shash, FHashMapIntList<N> hashtable, long expiration) {
        FHashSet<N> nodes = new FHashSet();
        for (int hash : shash) {
            ObjectArrayList<N> list = hashtable.get(hash);
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
                            nodes.add(n);
                        }
                    }
                }
            }
        }
        return nodes;
    }

    public FHashSet<N>[] getNodesTerms(N node, HashSet<String> terms, FHashMapList<String, N> termtable, long expiration) {
        FHashSet<N>[] nodes = new FHashSet[2];
        for (int i = 0; i < 2; i++) {
            nodes[i] = new FHashSet();
        }
        for (String term : terms) {
            ObjectArrayList<N> list = termtable.get(term);
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
                            if (nodes[0].contains(n)) {
                                nodes[1].add(n);
                            } else {
                                nodes[0].add(n);
                            }
                        }
                    }
                }
            }
        }
        return nodes;
    }

    public void addCandidates2(N node, FHashSet<N> candidates, FHashSetInt shash2, long expiration) {
        int countbefore = candidates.size();
        FHashSet<N> nodes = getNodes(node, shash2, bands2, expiration);
        candidates.addAll(nodes);
    }

    public void addCandidatesTerms(N node, FHashSet<N> candidates, HashSet<String> nodeterms, long expiration) {
        int countbefore = candidates.size();
        FHashSet<N>[] nodes = getNodesTerms(node, nodeterms, this.termmap, expiration);
        candidates.addAll(nodes[1]);
        if (candidates.size() < 10) {
            candidates.addAll(nodes[0]);
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
        Iterator<Object2ObjectMap.Entry<String, ObjectArrayList<N>>> iter2 = termmap.object2ObjectEntrySet().iterator();
        while (iter2.hasNext()) {
            Object2ObjectMap.Entry<String, ObjectArrayList<N>> next = iter2.next();
            ObjectArrayList<N> value = next.getValue();
            if (value.size() == 0 || value.get(value.size() - 1).getCreationTime() < expirationDatetime) {
                iter2.remove();
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
