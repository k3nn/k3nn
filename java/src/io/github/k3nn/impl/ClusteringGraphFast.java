package io.github.k3nn.impl;

import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashMapLongInt;
import io.github.htools.lib.Log;
import io.github.htools.fcollection.FHashSet;
import io.github.k3nn.Cluster;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.Edge;
import io.github.k3nn.Node;
import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class ClusteringGraphFast<N extends Node> extends ClusteringGraph<N> {

    public static final Log log = new Log(ClusteringGraphFast.class);

    /**
     * Add nodes to the ClusteringGraph, assigning nearest neighbors but without
     * forming clusters.
     * @param node
     * @param terms 
     */
    public void addWithoutClustering(N node, Collection<String> terms) {
        ArrayMapDouble<N> map = nodeStore.addGetList(node, terms);
        nodeStore.getNodes().put(node.getID(), node);

        for (Double2ObjectMap.Entry<N> entry : map) {
            if (entry.getDoubleKey() > node.getLowestScore()) {
                node.add(new Edge(entry.getValue(), entry.getDoubleKey()));
            }
            if (entry.getDoubleKey() > entry.getValue().getLowestScore()) {
                entry.getValue().add(new Edge(node, entry.getDoubleKey()));
            }
        }
    }
/**
 * Scan all unclustered nodes for existing 2-degenerate cores and construct
 * clusters around cores.
 * @param oldclusters 
 */
    public void reconstructClusters(FHashMapLongInt oldclusters) {
        clusters = new HashMap();
        for (N node : nodeStore.getNodes().values()) {
            node.setCluster(null);
        }
        for (N node : nodeStore.getNodes().values()) {
            if (node.getCluster() == null) { 
                // attempt to find a 2-degenerate core this node is part of
                FHashSet<N> coreNodes = (FHashSet<N>)node.get2DegenerateCore();
                if (coreNodes.size() >= Cluster.K) { // if a core exists
                    int clusterid = -1;
                    for (Node coreNode : coreNodes) {
                        if (oldclusters.containsKey(coreNode.getID())) {
                            clusterid = oldclusters.get(coreNode.getID());
                            break;
                        }
                    }
                    if (clusterid == -1) {
                        clusterid = this.nextclusterid++;
                    }
                    // create a new cluster and assign members using majority votes
                    Cluster<N> c = Cluster.createCoreCluster(this, clusterid, coreNodes);
                    assignNodes(c);
                    this.clusters.put(clusterid, c);
                }
            }
        }
    }

    /**
     * Fast version of assignNodes, no rechecking need for affected existing clusters
     * @param cluster 
     */
    public void assignNodes(Cluster<N> cluster) {
        // map to collect which nodes have a majority of nearest neighbors
        // that is assigned to the cluster
        HashMapInt<N> node2Votes = new HashMapInt();
        // the newest assigned nodes used to cast votes
        FHashSet<N> newnodes = new FHashSet(cluster.getCore());
        // currently assigned members
        FHashSet<N> members = new FHashSet(cluster.getCore());
        while (newnodes.size() > 0) {            
            // unassigned nodes that have a new cluster member as NN receive a vote
            for (Node node : newnodes) {
                if (node.getBacklinks() != null) {
                    for (Node nn : node.getBacklinks()) {
                        if (!nn.isClustered() && !members.contains(nn)) {
                            // increase the count of clustered nearest neighbor for node nn.
                            node2Votes.add((N) nn, 1);
                        }
                    }
                }
            }
            
            // Scan if unassigned nodes have a majority of NN in the cluster
            // and assign these as new members
            newnodes = new FHashSet();
            Iterator<Map.Entry<N, Integer>> iter = node2Votes.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<N, Integer> next = iter.next();
                N node = next.getKey();
                if (next.getValue() > Cluster.BREAKTIE) {
                    iter.remove();
                    if (!members.contains(node)) {
                        members.add(node);
                        node.setCluster(cluster);
                        newnodes.add(node);
                    }
                }
            }
        }
    }
}
