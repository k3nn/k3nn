package io.github.k3nn;

import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
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
 *
 * @author jeroen
 */
public class Cluster<N extends Node> {

    public static final Log log = new Log(Cluster.class);
    // maximum number of nearest neighbors, here fixed to 3
    public static final int K = 3;
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
    private static HashSet<Integer> watchlist = new HashSet(Arrays.asList(
            6915, 6907
    ));
    public final boolean watch;

    public Cluster(ClusteringGraph stream, int id) {
        this.clusteringGraph = stream;
        this.id = id;
        watch = watch(this);
    }

    /**
     * Only for debug purposes
     */
    public static Cluster createCluster() {
        return new Cluster(null, 0);
    }

    // cluster used by ClusteringGraph to construct a new cluster around a
    // 2-degenerate core
    public static <N extends Node> Cluster createCoreCluster(ClusteringGraph<N> clusteringGraph, int id, Collection<N> core) {
        Cluster<N> cluster = new Cluster(clusteringGraph, id);
        cluster.setCore(core);
        for (N coreNode : core) {
            coreNode.setCluster(cluster);
        }

        if (cluster.watch) {
            log.info("create %d", cluster.getID());
            for (Node coreNode : cluster.getCore()) {
                log.info("core %d %s %s", coreNode.getID(), coreNode.getNearestNeighborIds(), coreNode.getNearestNeighborScores());
            }
        }
        return cluster;
    }

    private static boolean watch(Cluster c) {
        return (watchlist.contains(c.getID()));
    }

    /**
     * Sets the core nodes without updating the cluster members
     *
     * @param core
     */
    public void setCore(Collection<? extends Node> core) {
        if (watch(this)) {
            log.info("setBase %d %s", id, core);
        }
        if (core == null) {
            this.core = null;
        } else if (core.size() < Cluster.K) {
            log.fatal("cluster %d base size %d", getID(), core.size());
        } else {
            this.core = new FHashSet(core);
        }
    }

    /**
     * Sets the member nodes.
     *
     * @param nodes
     */
    public void setNodes(Collection<? extends Node> nodes) {
        //log.info("setNodes %d %d", id, nodes.size());
        this.nodes = new ArrayList(nodes);
    }

    /**
     * If there is a 2-degenerate core in the members nodes, it finds it and
     * sets this as the core. If no core is found, the cluster is disbanded, all
     * nodes are set to an unclustered status and the cluster is removed from
     * the ClusteringGraph.
     *
     * @return true if 2-degenerate core was found
     */
    public boolean find2DegenerateCore() {
        FHashSet<N> core = get2DegenerateCore(nodes);
        //if (watch(this)) {
        //log.info("find2DegenerateCore %s", core);
        //}
        if (core.isEmpty()) {
            // no 2-degenerate core was found -> disband
            for (int i = nodes.size() - 1; i >= 0; i--) {
                nodes.get(i).setCluster(null);
            }
            nodes = null;
            this.core = null;
            clusteringGraph.remove(this);
            if (watch(this)) {
                log.info("find2DegenerateCore remove cluster %s", core);
            }
            return false;
        } else {
            if (!core.equals(this.core)) {
                //log.info("find2DegenerateCore news core %s", core);
                setCore(core);
            }
            return true;
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

    public void addNode(N node) {
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
     * @return the 2-degenerate core of linked nearest neighbor nodes within the
     * set of nodes, not looking outside the given set, and returning an empty
     * list when no 2-degenerate core exists.
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

    public static <N extends Node> FHashSet<N> getCoreFast(Collection<N> nodes) {
        for (N u : nodes) {
            int count = Cluster.BREAKTIE;
            for (int e = u.edges - 1; e >= count; e--) {
                Edge<N> edge = u.getNearestNeighbor(e);
                N to = edge.getNode();
                if (to != null) {
                    if (to.getID() > u.getID() && to.linkedTo(u)) {
                        if (count-- == 0) {
                            FHashSet<N> base1 = (FHashSet<N>) u.get2DegenerateCore();
                            if (base1.size() > 0) {
                                return base1;
                            }
                            break;
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

    public int hashCode() {
        return id;
    }

    public boolean equals(Object o) {
        return this == o;
    }

    /**
     * remove nodes that are not linked transitively linked to from the cluster
     * base
     *
     * @param cluster
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
