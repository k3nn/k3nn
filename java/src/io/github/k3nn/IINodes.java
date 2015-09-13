package io.github.k3nn;

import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.fcollection.FHashMapList;
import io.github.htools.fcollection.FHashMapLongObject;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * An inverted index to retrieve nodes that contain a feature.
 *
 * @author jeroen
 */
public abstract class IINodes<N extends Node> extends FHashMapList<String, N> implements NodeStore<N> {

    public static final Log log = new Log(IINodes.class);
    protected FHashMapLongObject<N> nodes = new FHashMapLongObject();

    /**
     * Add a node to the postings lists of the terms
     *
     * @param node
     * @param terms
     */
    @Override
    public void add(N node, Collection<String> terms) {
        for (String term : terms) {
            add(term, node);
        }
    }

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list
     * they appear in
     */
    @Override
    public abstract ArrayMapDouble<N> addGetList(N node, Collection<String> terms);

    /**
     * removes expired nodes from the node pool and indexes, if the node is still
     * referred to from a cluster, it will still be accessible through the cluster
     * but no longer through the NodeStore.
     * @param expirationDatetime 
     */
    @Override
    public void purge(long expirationDatetime) {
        Iterator<ObjectArrayList<N>> termIterator = this.values().iterator();
        while (termIterator.hasNext()) {
            ObjectArrayList<N> nodeList = termIterator.next();
            if (nodeList.size() == 0 || nodeList.get(nodeList.size() - 1).getCreationTime() < expirationDatetime) {
                // remove the term, the last node in the last has expired
                termIterator.remove();
            } else {
                Iterator<N> nodeIterator = nodeList.iterator();
                while (nodeIterator.hasNext()) {
                    N node = nodeIterator.next();
                    if (node.creationtime < expirationDatetime) {
                        nodeIterator.remove();
                    } else {
                        break;
                    }
                }
            }
        }
        Iterator<N> nodeIterator = getNodes().values().iterator();
        while (nodeIterator.hasNext()) {
            N node = nodeIterator.next();
            if (!node.isClustered() && node.getCreationTime() < expirationDatetime) {
                nodeIterator.remove();
            }
        }
    }

    /**
     * @return a map of all non-expired nodes kept in store
     */
    @Override
    public FHashMapLongObject<N> getNodes() {
        return nodes;
    }
}
