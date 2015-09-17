package io.github.k3nn;

import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import static io.github.htools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * A Cluster is a set of Nodes that is grouped together, typically by a
 * ClusteringGraph using the 3NN heuristic. The core nodes of the cluster is
 * typically a subset of the cluster member nodes that form a 2-degenerate core
 * of a 3 nearest neighbor graph.
 *
 * @author jeroen
 */
public class Cluster<N extends Node> {

    public static final Log log = new Log(Cluster.class);

    // maximum number of nearest neighbors, here fixed to 3
    public static final int K = 3;

    // for assignment of non-core nodes, typically majority votes is used, i.e. 
    // assigning nodes to the same number of nodes as their majority of NNs, or
    // not assigning if no majority exists. BREAKTIE gives the number to exceed 
    // to obtain a majority.
    public static final int BREAKTIE = K / 2;

    // internal cluster ID
    final int id;
    private ClusteringGraph clusteringGraph;
    // all cluster members, consisting of core nodes and other assigned nodes
    // using a heuristic such as majority votes
    private ArrayList<N> nodes = new ArrayList();
    // the 2-degenerate core nodes that form the cluster
    private FHashSet<N> core = new FHashSet();
    // for debugging purposes
    private static HashSet<Integer> watchlist = new HashSet(Arrays.asList());
    // for debugging purposes, set to true to trace information
    public final boolean watch;

    public Cluster(ClusteringGraph clusteringGraph, int id) {
        this.clusteringGraph = clusteringGraph;
        this.id = id;
        watch = watch(this);
    }

    // used by ClusteringGraph to construct a new cluster around a
    // 2-degenerate core
    public static <N extends Node> Cluster createCoreCluster(ClusteringGraph<N> clusteringGraph, int id, Collection<N> core) {
        Cluster<N> cluster = new Cluster(clusteringGraph, id);
        cluster.setCore(core);
        for (N coreNode : core) {
            coreNode.setCluster(cluster);
        }

//        if (cluster.watch) {
//            log.info("create %d", cluster.getID());
//            for (Node coreNode : cluster.getCore()) {
//                log.info("core %d %s %s", coreNode.getID(), coreNode.getNearestNeighborIds(), coreNode.getNearestNeighborScores());
//            }
//        }
        return cluster;
    }

    /**
     * @return a shallow copy of the current cluster with the same core nodes.
     * experimental, don't use unless you known what you are doing.
     */
    public Cluster<N> cloneCore() {
        return createCoreCluster(clusteringGraph, id, getCore());
    }

    private static boolean watch(Cluster c) {
        return (watchlist.contains(c.getID()));
    }

    /**
     * Sets the core nodes without updating the cluster members
     *
     * @param core
     */
    void setCore(Collection<? extends Node> core) {
        if (watch(this)) {
            log.info("setCore %d %s", id, core);
        }
        if (core == null) {
            this.core = null;
        } else if (core.size() < Cluster.K) {
            log.fatal("cluster %d invalid core too small %d", getID(), core.size());
        } else {
            this.core = new FHashSet(core);
        }
    }

    /**
     * @return based on the correct core nodes, assess which nodes should be a
     * member of this cluster based on a majority (2) of their nearest neighbors
     * being a member of the same cluster.
     */
    public FHashSet<N> findMajorityMembers() {
        FHashSet<N> nodes = new FHashSet(getCore());
        FHashSet<N> newnodes = new FHashSet(getCore());
        HashMapInt<N> votes = new HashMapInt();
        while (newnodes.size() > 0) {
            for (N u : newnodes) {
                if (u.backlinks != null) {
                    for (Node b : u.backlinks) {
                        if (!nodes.contains(b)) {
                            votes.add((N) b, 1);
                        }
                    }
                }
            }
            newnodes = new FHashSet();
            Iterator<Map.Entry<N, Integer>> iter = votes.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<N, Integer> next = iter.next();
                N u = next.getKey();
                if (next.getValue() > Cluster.BREAKTIE) {
                    iter.remove();
                    nodes.add(u);
                    newnodes.add(u);
                }
            }
        }
        return nodes;
    }

    /**
     * @return internal cluster ID
     */
    public int getID() {
        return id;
    }

    /**
     * Adds the node as a cluster member (not a core node). The node's cluster
     * assignment is not updated, but this method is commonly only called by the
     * node which should update its own reference.
     *
     * @param node
     */
    void addNode(N node) {
        if (watch(this)) {
            log.info("Add Url %d to cluster %d", node.getID(), getID());
        }
        nodes.add(node);
    }

    /**
     * @return A list of members assigned to this cluster
     */
    public ArrayList<N> getNodes() {
        return nodes;
    }

    /**
     * @return the assigned 2-degenerate core nodes of the cluster, does not
     * validate the correctness of the core
     */
    public FHashSet<N> getCore() {
        return core;
    }

    /**
     * @param node
     * @return true if one of the cluster's members had the node as a nearest
     * neighbor
     */
    public boolean linkedTo(Node node) {
        for (Node u : nodes) {
            if (u.linkedTo(node)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param node
     * @return true if there exists a directed path from a core node to the
     * given node
     */
    public boolean linkedFromCore(Node node) {
        HashSet<Node> seenlinks = new HashSet();
        HashSet<Node> newnodes = new HashSet(core);
        while (newnodes.size() > 0) {
            seenlinks.addAll(newnodes);
            HashSet<Node> newnewnodes = new HashSet();
            for (Node newnode : newnodes) {
                for (int neighborIndex = 0; neighborIndex < newnode.edges; neighborIndex++) {
                    Node nearestNeighbor = newnode.getNearestNeighbor(neighborIndex).node;
                    if (nearestNeighbor != null && !seenlinks.contains(nearestNeighbor) && nearestNeighbor.getCluster() == this) {
                        if (nearestNeighbor == node) {
                            return true;
                        }
                        newnewnodes.add(nearestNeighbor);
                    }
                }
            }
            newnodes = newnewnodes;
        }
        return false;
    }

    /**
     * removes the node from it's members without checking if the node is part
     * of the core.
     *
     * @param node
     */
    public void remove(Node node) {
        getNodes().remove(node);
    }

    public static final FHashSet emptylist = new FHashSet();

    /**
     * @param nodes
     * @return the 2-degenerate core of linked nearest neighbor nodes with at
     * least one member inside the set of nodes, and returning an empty list
     * when no 2-degenerate core exists. When multiple cores exists, only one is
     * returned.
     */
    public static <N extends Node> FHashSet<N> get2DegenerateCore(Collection<N> nodes) {
        for (N node : nodes) {
            int count = 0;
            for (int e = 0; e < node.edges; e++) {
                Edge<N> edge = node.getNearestNeighbor(e);
                N to = edge.getNode();
                if (to != null) {
                    if (to.getID() > node.getID() && nodes.contains(to) && to.linkedTo(node)) {
                        if (++count > Cluster.BREAKTIE) {
                            FHashSet<N> base1 = (FHashSet<N>) node.get2DegenerateCore();
                            if (base1.size() > 0) {
                                return base1;
                            }
                        }
                    }
                }
            }
        }
        return emptylist;
    }

    /**
     * @param nodes
     * @return the 2-degenerate core of linked nearest neighbor nodes within the
     * set of nodes, not looking outside the given set, and returning an empty
     * list when no 2-degenerate core exists. This method should be used when
     * reconstructing serialized clusters.
     */
    public static FHashSet<? extends Node> get2DegenerateCoreWithin(Collection<? extends Node> nodes) {
        for (Node node : nodes) {
            int count = 0;
            for (int e = 0; e < node.edges; e++) {
                Edge<Node> edge = node.getNearestNeighbor(e);
                Node to = edge.getNode();
                if (to != null) {
                    if (to.getID() > node.getID() && nodes.contains(to) && to.linkedTo(node)) {
                        if (++count > Cluster.BREAKTIE) {
                            FHashSet<Node> base1 = node.get2DegenerateCoreWithin(nodes);
                            if (base1.size() > 0) {
                                return base1;
                            }
                        }
                    }
                }
            }
        }
        return emptylist;
    }

    /**
     * @return number of members of this cluster
     */
    public int size() {
        return nodes.size();
    }

    @Override
    public int hashCode() {
        return MathTools.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        // of every cluster there should be only a single instance
        return this == o;
    }

    /**
     * removes all cluster members to which there is no directed path from a 
     * core node.
     */
    public void stripCluster() {
        FHashSet<N> urls = baseLinkedCluster();
        for (int i = nodes.size() - 1; i >= 0; i--) {
            N node = nodes.get(i);
            if (!urls.contains(node)) {
                node.setCluster(null);
            }
        }
    }

    /**
     * @return a set of nodes that are either core nodes or nodes to which a
     * directed path from a core node exists.
     */
    public FHashSet<N> baseLinkedCluster() {
        FHashSet<N> urls = new FHashSet(nodes.size());
        FHashSet<N> newurls = getCore();
        while (newurls.size() > 0) {
            urls.addAll(newurls);
            FHashSet<N> nextbatch = new FHashSet();
            for (N u : newurls) {
                for (int e = 0; e < u.countNearestNeighbors(); e++) {
                    N l = (N) u.getNearestNeighbor(e).getNode();
                    if (l != null && !urls.contains(l) && l.getCluster() == this) {
                        nextbatch.add(l);
                    }
                }
            }
            newurls = nextbatch;
        }
        return urls;
    }

    @Override
    public String toString() {
        //return Integer.toString(id);
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("\ncluster %d [%d] ", getID(), nodes.size()));
        for (Node node : nodes) {
            sb.append("\n node ").append(node.toStringEdges());
        }
        for (Node node : core) {
            sb.append("\n base ").append(node.toStringEdges());
        }
        return sb.toString();
    }

    public String evalall() {
        StringBuilder sb = new StringBuilder();
        sb.append(eval()).append("\n-----\n");
        HashSet<Node> misplaced = new HashSet(misplaced());
        ArrayList<N> sortedNodes = new ArrayList(getNodes());
        Collections.sort(sortedNodes, IdComparator.getInstance());
        for (N n : sortedNodes) {
            if (!getCore().contains(n) && !misplaced.contains(n)) {
                sb.append(n.getID()).append(": ");
                addEval(sb, n);
            }
        }
        if (misplaced.size() > 0) {
            sortedNodes = new ArrayList(misplaced);
            Collections.sort(sortedNodes, IdComparator.getInstance());
            sb.append("\n---- misplaced ---\n");
            for (N u : sortedNodes) {
                sb.append(u.getID()).append(": ");
                addEval(sb, u);
            }
        }
        return sb.toString();
    }

    public ArrayList<N> misplaced() {
        HashSet<Node> covered = new HashSet(getCore());
        HashSet<Node> todo = new HashSet(getNodes());
        todo.removeAll(covered);
        int size = todo.size() + 1;
        while (size > todo.size()) {
            size = todo.size();
            Iterator<Node> iter = todo.iterator();
            while (iter.hasNext()) {
                Node u = iter.next();
                int count = 0;
                for (int i = 0; i < u.edges; i++) {
                    Node nn = u.getNearestNeighbor(i).getNode();
                    if (nn != null && covered.contains(nn)) {
                        count++;
                    }
                }
                if (count > Cluster.BREAKTIE) {
                    iter.remove();
                    covered.add(u);
                }
            }
        }
        ArrayList<N> misplaced = new ArrayList(getNodes());
        misplaced.removeAll(covered);
        return misplaced;
    }

    public String eval() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("CLUSTER %d size=%d\n", id, nodes.size()));
        for (N coreNode : core) {
            sb.append(coreNode.getID()).append(":");
            addEval(sb, coreNode);
        }
        return sb.toString();
    }

    private void addEval(StringBuilder sb, N u) {
        LOOP:
        for (Edge<N> edge : new EdgeIterator<N>(u)) {
            Node l = edge.getNode();
            if (l != null) {
                sb.append(l.getID()).append(" ");
            } else {
                sb.append("x ");
            }
        }
        for (Edge<N> edge : new EdgeIterator<N>(u)) {
            sb.append(edge.score).append(" ");
        }
        sb.append("\n");
    }

    /**
     * A Comparator to sort clusters on id.
     */
    public static class IdComparator implements Comparator<Node> {

        private static IdComparator instance;

        public static IdComparator getInstance() {
            if (instance == null) {
                instance = new IdComparator();
            }
            return instance;
        }

        @Override
        public int compare(Node o1, Node o2) {
            return (int) (o1.getID() - o2.getID());
        }

    }
}
