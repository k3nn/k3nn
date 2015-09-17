package io.github.k3nn;

import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.fcollection.FHashMapLongObject;
import java.util.Collection;

/**
 * Stores the nodes for a clustering graph to allow retrieving nodes based on 
 * ID and a list of (potential) nearest neighbor nodes for a given node. Implementations
 * should use some strategy to compile such a list, e.g. an inverted index or 
 * locality sensitive hashing, and contains a scoring function to assign scores
 * to the retrieved list.
 * @author jeroen
 * @param <N>
 */
public interface NodeStore<N extends Node> {

    /**
     * Add a node to the postings lists of the terms
     * @param node
     * @param terms 
     */
    public void add(N node, Collection<String> terms);

    public FHashMapLongObject<N> getNodes();
    
    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list they 
     * appear in
     */
    public abstract ArrayMapDouble<N> addGetList(N node, Collection<String> terms);
    
    /**
     * removes expired nodes from the node pool and indexes, if the node is still
     * referred to from a cluster, it will still be accessible through the cluster
     * but no longer through the NodeStore.
     * @param expirationDatetime 
     */
    public void purge(long expirationDatetime);
}
