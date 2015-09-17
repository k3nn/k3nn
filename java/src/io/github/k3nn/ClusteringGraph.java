package io.github.k3nn;

import io.github.k3nn.impl.NodeStoreIIBinary;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.extract.modules.RemoveFilteredWords;
import io.github.htools.extract.modules.StemTokens;
import io.github.htools.extract.modules.TokenToLowercase;
import io.github.htools.lib.Log;
import io.github.htools.words.StopWordsMultiLang;
import io.github.htools.extract.modules.LStemTokens;
import io.github.htools.fcollection.FHashMapLongObject;
import io.github.htools.fcollection.FHashSet;
import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author jeroen
 */
public class ClusteringGraph<N extends Node> {

    public static final Log log = new Log(ClusteringGraph.class);

    static DefaultTokenizer tokenizer = getUnstemmedTokenizer();
    public HashMap<Integer, Cluster<N>> clusters = new HashMap();
    public NodeStore<N> nodeStore;
    protected int nextclusterid = 0;
    // the number of days after which nodes expire, usually one day more than
    // the time interval that assigns a 0 similarity to nodes (3 days by default)
    protected int expirationDays = 4;

    public ClusteringGraph() {
        nodeStore = createII();
    }

    public void setNextClusterID(int id) {
        nextclusterid = id;
    }

    protected NodeStore createII() {
        return new NodeStoreIIBinary();
    }

    public static DefaultTokenizer getStemmedTokenizer() {
        HashSet<String> stemmedFilterSet = StopWordsMultiLang.get().getStemmedFilterSet();
        DefaultTokenizer tokenizer = new DefaultTokenizer();
        try {
            tokenizer.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        tokenizer.addEndPipeline(StemTokens.class);
        tokenizer.addEndPipeline(new RemoveFilteredWords(tokenizer, stemmedFilterSet));
        return tokenizer;
    }

    public static DefaultTokenizer getLancasterStemmedTokenizer() {
        HashSet<String> stemmedFilterSet = StopWordsMultiLang.get().getLancasterStemmedFilterSet();
        DefaultTokenizer tokenizer = new DefaultTokenizer();
        try {
            tokenizer.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        tokenizer.addEndPipeline(LStemTokens.class);
        tokenizer.addEndPipeline(new RemoveFilteredWords(tokenizer, stemmedFilterSet));
        return tokenizer;
    }

    public static DefaultTokenizer getUnstemmedTokenizer() {
        HashSet<String> unstemmedFilterSet = StopWordsMultiLang.get().getUnstemmedFilterSet();
        DefaultTokenizer tokenizer = new DefaultTokenizer();
        try {
            tokenizer.getTokenprocessor().addSubProcessor(TokenToLowercase.class);
        } catch (ClassNotFoundException ex) {
            log.exception(ex);
        }
        //t.addEndPipeline(StemTokens.class);
        tokenizer.addEndPipeline(new RemoveFilteredWords(tokenizer, unstemmedFilterSet));
        return tokenizer;
    }

    /**
     * @param core set of nodes that form a 2-degenerate core
     * @return create new cluster with the given 2-degenerate core.
     */
    public Cluster<N> createClusterFromCore(Collection<N> core) {
        return createClusterFromCore(nextclusterid, core);
    }

    /**
     * @param id new cluster id
     * @param core set of nodes that form a 2-degenerate core
     * @return create cluster with given id and set of 2-degenerate core nodes,
     * the validity of the core is not checked.
     */
    public Cluster<N> createClusterFromCore(int id, Collection<N> core) {
        Cluster<N> c = Cluster.createCoreCluster(this, id, core);
        if (id >= nextclusterid) {
            nextclusterid = id + 1;
        }
        clusters.put(c.getID(), c);
        return c;
    }

    /**
     * reconstructs cluster from a set of cluster members, finding a 2
     * degenerate core within its members and assigning the other nodes as
     * members. This method is designed to work on valid clusters that have been
     * serialized and does not check whether the nodes are valid members.
     *
     * @param id
     * @param nodes
     * @return a cluster when successful or NULL when no core exists.
     */
    public Cluster<N> createClusterFromSet(int id, Collection<N> nodes) {
        FHashSet<N> core = (FHashSet<N>) Cluster.get2DegenerateCoreWithin(nodes);
        if (core.size() >= Cluster.K) {
            Cluster<N> cluster = createClusterFromCore(id, core);
            for (N node : nodes) {
                node.setCluster(null); // to be sure nodes are added in the same order
                node.setCluster(cluster);
            }
            return cluster;
        } else {
            log.info("illegal cluster %d", id);
        }
        return null;
    }

    /**
     * @return collection of clusters in the graph
     */
    public Collection<Cluster<N>> getClusters() {
        return clusters.values();
    }

    /**
     * sets the next cluster id, allowing to continue clustering using a
     * snapshot.
     *
     * @param nextclusterid
     */
    public void setStartClusterID(int nextclusterid) {
        this.nextclusterid = Math.max(this.nextclusterid, nextclusterid);
    }

    /**
     * @param id cluster ID
     * @return the cluster with the given internal clusterID
     */
    public Cluster getCluster(int id) {
        return clusters.get(id);
    }

    /**
     * Removes the cluster from the graph, applicable when entire clusters
     * expire. However, the members are not detached from the cluster, so use
     * with care.
     *
     * @param cluster
     */
    protected void remove(Cluster cluster) {
        clusters.remove(cluster.getID());
    }

    /**
     * Add a new node to the clustering Graph, updating all nearest neighbor
     * edges and the clustering. If isCandidate is true and the node is
     * clustered immediately, addSentence is called.
     *
     * @param newNode the node that is added to the graph.
     * @param terms tokens in the node's content, used to compute similarity
     * between nodes.
     * @param isCandidate
     */
    public void addNode(N newNode, HashSet<String> terms, boolean isCandidate) {
        addNode(newNode, terms);
        if (newNode.isClustered() && isCandidate) {
            clusteredCandidateSentence(newNode);
        }
    }

    /**
     * This method is called when a new candidate node is clustered immediately
     * using addSentence() which can be captured by overriding this method.
     *
     * @param candidateSentence
     */
    public void clusteredCandidateSentence(N candidateSentence) {
    }

    /**
     * Add a new node to the clustering Graph, updating all nearest neighbor
     * edges and the clustering.
     *
     * @param newNode the node that is added to the graph.
     * @param terms tokens in the node's content, used to compute similarity
     * between nodes.
     */
    public void addNode(N newNode, Collection<String> terms) {
        // clusters for which one of the core nodes was updated, so recheck these clusters
        FHashSet<Cluster> coreUpdatedClusters = new FHashSet();
        // clusters for which a non-core node was updated, so reassign additional members
        FHashSet<Cluster> updatedClusters = new FHashSet();

        // add newNode to NodeStore, and retrieve a list of nodes with similarity scores
        ArrayMapDouble<N> score2Node = nodeStore.addGetList(newNode, terms);
        nodeStore.getNodes().put(newNode.getID(), newNode);

        // update the nearest neigbor edges
        updateNearestNeigbors(newNode, score2Node, coreUpdatedClusters, updatedClusters);

        // recheck the clusters for which a core node was updated
        for (Cluster c : coreUpdatedClusters) {
            recheck(updatedClusters, c);
        }

        // check if the newNode is part of a new 2-degenerate core (create cluster)
        // or assign it to the majority cluster of its nearest neighbor or leave
        // it unclustered when no majority exists.
        clusterNewNode(newNode, updatedClusters);

        // update coreUpdatedClusters (disband when there is no valid core or change
        // updated cores) and update updatedClusters (reassign non-corr members).
        updateClusters(coreUpdatedClusters, updatedClusters);
    }

    /**
     * Update the nearest neighbors using the list of scored nodes.
     *
     * @param newNode the node that is currently added to the graph
     * @param score2Node a list of scored (potential) nearest neighbor nodes
     * @param coreUpdatedClusters clusters for which one of the core nodes was
     * updated are added
     * @param updatedClusters clusters for which one of the non-core members was
     * updated are added
     */
    protected void updateNearestNeigbors(N newNode, ArrayMapDouble<N> score2Node,
            FHashSet<Cluster> coreUpdatedClusters, FHashSet<Cluster> updatedClusters) {
        // note: for TREC we used > node.getLowestScore(), for ICTIR >=
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
                        coreUpdatedClusters.add(cluster);
                    } else if (changedNode.isClustered()) {
                        updatedClusters.add(changedNode.getCluster());
                    }
                }
            }
        }
    }

    /**
     * First check if the new node is part of a 2-degenerate core, and if so
     * create a new cluster, otherwise the new node is assigned to the same
     * cluster as the majority of its nearest neighbors. When the newNode is
     * clustered, the cluster is added to updatedClusters to reassign non-core
     * members.
     *
     * @param newNode
     * @param updatedClusters set of updated clusters for which the members must
     * be reassigned.
     */
    protected void clusterNewNode(N newNode, FHashSet<Cluster> updatedClusters) {
        if (!newNode.isClustered()) {
            Cluster newcluster = attemptNewCluster(newNode, updatedClusters);
            if (newcluster != null) {
                updatedClusters.add(newcluster);
            } else {
                // no new cluster was formed, so attempt to assign to majority
                newNode.setCluster(newNode.majority());
                if (newNode.isClustered()) {
                    updatedClusters.add(newNode.getCluster());
                }
            }
        }
    }

    /**
     * Revalidates an existing cluster for which a core node was updated. If no
     * 2-degenerate core is found, the cluster is disbanded, if a different core
     * is found than the existing core, the core is updated and the cluster is
     * added to the list of updatedClusters for reassignment of non-core
     * members.
     *
     * @param updatedClusters set of cluster for which a non-core member was updated
     * @param cluster
     */
    protected void recheck(FHashSet<Cluster> updatedClusters, Cluster cluster) {
        FHashSet<Node> core = findCoreNodes(cluster);
        if (core.isEmpty()) {
            ArrayList<Node> nodes = new ArrayList(cluster.getNodes());
            for (Node node : nodes) {
                node.setCluster(null);
            }
            remove(cluster);
        } else if (!cluster.getCore().equals(core)) {
            cluster.setCore(core);
            for (Node node : new ArrayList<Node>(cluster.getNodes())) {
                node.setCluster(null);
            }
            for (Node node : core) {
                node.setCluster(cluster);
            }
            updatedClusters.add(cluster);
        }
    }

    /**
     * Iteratively check recheckClusters and updatedCluster, reassigning members
     * to changed clusters and continuing as long as there are newly updated
     * clusters.
     *
     * @param coreUpdatedClusters clusters for which a core-node was updated
     * @param updatedClusters clusters for which a non-core member was updated
     */
    protected void updateClusters(FHashSet<Cluster> coreUpdatedClusters,
            FHashSet<Cluster> updatedClusters) {
        if (updatedClusters.size() > 0) {
            FHashSet<Cluster> alreadyUpdated = new FHashSet();
            FHashSet<Cluster> newrecheckClusters = new FHashSet();
            FHashSet<Cluster> newUpdateClusters = new FHashSet();
            while (updatedClusters.size() > 0) {
                for (Cluster c : updatedClusters) {
                    if (clusters.containsKey(c.id)) {
                        assignNodes(newrecheckClusters, newUpdateClusters, c);
                    }
                }
                // recheck clusters from which a core node was pulled
                newrecheckClusters.removeAll(coreUpdatedClusters);
                for (Cluster c : newrecheckClusters) {
                    recheck(newUpdateClusters, c);
                }
                // there are updated clusters, possibly need another iteration
                if (newUpdateClusters.size() > 0) {
                    alreadyUpdated.addAll(updatedClusters);
                    newUpdateClusters.removeAll(alreadyUpdated);
                    updatedClusters = newUpdateClusters;
                    // definetely need another iteration to reassign members to updated clusters
                    if (updatedClusters.size() > 0) {
                        coreUpdatedClusters.addAll(newrecheckClusters);
                        newrecheckClusters = new FHashSet();
                        newUpdateClusters = new FHashSet();
                    }
                } else {
                    updatedClusters = newUpdateClusters;
                }
            }
        }
    }

    private static FHashSet<Node> emptyList = new FHashSet<Node>();

    /**
     * @param cluster
     * @return set of nodes that form a 2-degenerate core, that includes one of
     * the clusters nodes but may also contain currently unclustered nodes, or
     * an empty set if no core exists.
     */
    protected static FHashSet<Node> findCoreNodes(Cluster cluster) {
        for (Node node : (ArrayList<Node>) cluster.getNodes()) {
            if (node.hasTwoBidirectedEdges()) {
                FHashSet<Node> coreNodes = node.get2DegenerateCore();
                if (coreNodes.size() > Cluster.BREAKTIE) {
                    return coreNodes;
                }
            }
        }
        return emptyList;
    }

    /**
     * Assigns nodes as member to a cluster when they have a majority of nearest
     * neighbors in the cluster (i.e. two nearest neighbors are (indirectly)
     * linked to a core node). Current non-core members that no longer have a
     * majority in this cluster are unassigned.
     *
     * @param coreUpdatedClusters set of clusters for which a core member was updated
     * @param updateClusters set of cluster for which a non-core member was updated
     * @param cluster cluster containing core nodes that are used to assign
     * additional members.
     */
    protected static void assignNodes(FHashSet<Cluster> coreUpdatedClusters,
            FHashSet<Cluster> updateClusters,
            Cluster<? extends Node> cluster) {
        FHashSet<? extends Node> nodes = cluster.findMajorityMembers();
        FHashSet<Node> existing = new FHashSet<Node>(cluster.getNodes());
        existing.removeAll(nodes);
        for (Node node : existing) {
            node.setCluster(null);
        }
        for (Node node : nodes) {
            if (node.isClustered() && node.getCluster() != cluster) {
                // node is member in another cluster
                if (node.getCluster().getCore().contains(node)) {
                    // as core node, so recheck cluster
                    coreUpdatedClusters.add(node.getCluster());
                } else {
                    // as non-core member, so reassign members
                    updateClusters.add(node.getCluster());
                }
            }
            node.setCluster(cluster);
        }
    }

    /**
     * @param updatedClusters set of clusters for which a non-core member was updated
     * @param startNode
     * @return if the startNode is part of a 2-degenerate core (not disrupting
     * existing cluster cores), a new cluster is returned with this core,
     * otherwise NULL.
     */
    protected Cluster attemptNewCluster(N startNode, FHashSet<Cluster> updatedClusters) {
        if (startNode.hasTwoBidirectedEdges()) {
            FHashSet<N> core = (FHashSet<N>) startNode.get2DegenerateCore();
            if (core.size() > Cluster.BREAKTIE) {
                for (N node : core) {
                    if (node.isClustered() && node.getCluster().getCore().contains(node)) {
                        return null;
                    }
                }
                for (N node : core) {
                    if (node.isClustered()) {
                        updatedClusters.add(node.getCluster());
                    }
                }
                Cluster<N> newcluster = createClusterFromCore(core);
                updatedClusters.add(newcluster);
                return newcluster;
            }
        }
        return null;
    }

//    public Edge createEdge(N owner, N destination, double score) {
//        return new Edge(destination, score);
//    }
    /**
     * @param creationtime
     * @return expiration time for nodes that can be purged when no longer
     * useful
     */
    public long getExpirationTime(long creationtime) {
        return creationtime - expirationDays * 24 * 60 * 60;
    }

    /**
     * Removes nodes that have 'expired' and are no longer useful. Nodes have
     * expired when their similarity to new nodes is always zero because their
     * creation time is too far in the past (i.e. creation time &lt;
     * expirationTime). However, nodes that are members of clusters are kept
     * until all nodes in the cluster have expired.
     *
     * @param creationtime
     */
    public void purge(long creationtime) {
        long expirationTime = getExpirationTime(creationtime);
        nodeStore.purge(expirationTime);
        purgeClusters(expirationTime);
    }

    private void purgeClusters(long expirationTime) {
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

    /**
     * @return access to an id2Node map of nodes in the graph
     */
    public FHashMapLongObject<N> getNodes() {
        return nodeStore.getNodes();
    }

    /**
     * @param id node ID
     * @return the node with the current id, or NULL if non existent
     */
    public N getNode(long id) {
        return nodeStore.getNodes().get(id);
    }
}
