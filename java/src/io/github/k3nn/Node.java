package io.github.k3nn;

import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import static io.github.htools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A Node that is used for clustering. A node represents a piece of information,
 * e.g. a sentence, which is connected to its nearest neighbors based on
 * similarity in features (e.g. words, domain, creation time). The most basic
 * implementation of a Node is stripped from everything except the clustering
 * information, to minimize memory used. As such a cluster of Node will supply
 * only internal Node ID's, and information about assigned nearest neighbors and
 * cluster. Note that this class cannot be clustered since number of features is
 * missing. Other subclasses of Node have attributes to report more elaborately,
 * e.g. NodeM holds the number of terms to allow comparing similarity, NodeT
 * contains also the title.
 *
 * @author jeroen
 */
public abstract class Node implements Comparable<Node> {

    public static Log log = new Log(Node.class);
    public static HashSet<Long> watchlist = new HashSet(
            Arrays.asList(
                   
            )); // for debugging
    public final boolean watch; // used for debugging
    private long id;
    protected Edge[] edge = new Edge[Cluster.K]; // links to nearest neighbors
    protected int edges = 0; // number of edges used
    FHashSet<Node> backlinks; // nodes that have this node as nearest neighbor
    public int domain = -1;
    protected long creationtime;
    private Cluster cluster = null;

    public Node(long id) {
        this.id = id;
        watch = watch(this); // for debug purposes
    }

    public Node(long id, int domain, long creationtime) {
        this(id);
        this.domain = domain;
        this.creationtime = creationtime;
    }

    private static boolean watch(Node node) {
        return watchlist.contains(node.getID());
    }

    public long getID() {
        return id;
    }

    protected void clear() {
        edge = new Edge[Cluster.K];
        backlinks = null;
        cluster = null;
        edges = 0;
    }

    /**
     * @param i
     * @return the i-th nearest neighbor edge
     */
    public Edge getNearestNeighbor(int i) {
        return edge[i];
    }

    private void addBackLink(Node u) {
        if (backlinks == null) {
            backlinks = new FHashSet();
        }
        backlinks.add(u);
    }

    private void removeBackLink(Node u) {
        backlinks.remove(u);
    }

    public FHashSet<Node> getBacklinks() {
        return backlinks;
    }

    /**
     * add an Edge if the score is higher than the weakest NN, the NNs remain
     * sorted
     *
     * @param e
     */
    public void add(Edge e) {
        if (edges == 0) {
            edge[edges++] = e;
            if (e.getNode() != null) {
                ((Node) e.getNode()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d 0 %d %f", getID(), e.getNode() == null ? -1 : e.getNode().getID(), e.getScore());
            }
        } else if (edge[edges - 1].getScore() <= e.getScore()) {
            if (edges == Cluster.K && edge[Cluster.K - 1].getNode() != null) {
                ((Node) edge[Cluster.K - 1].getNode()).removeBackLink(this);
            }
            for (int pos = 0; pos < edges; pos++) {
                if (edge[pos].getScore() <= e.getScore()) {
                    for (int i = Math.min(Cluster.K - 1, edges); i > pos; i--) {
                        edge[i] = edge[i - 1];
                    }
                    edge[pos] = e;
                    if (e.getNode() != null) {
                        ((Node) e.getNode()).addBackLink(this);
                    }
                    if (watch) {
                        log.info("add Edge %d %d %d %f", getID(), pos, e.getNode() == null ? -1 : e.getNode().getID(), e.getScore());
                    }
                    if (edges < Cluster.K) {
                        edges++;
                    }
                    return;
                }
            }
        } else if (edges < Cluster.K) {
            edge[edges++] = e;
            if (e.getNode() != null) {
                ((Node) e.getNode()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d %d %d %f", getID(), edges - 1, e.getNode() == null ? -1 : e.getNode().getID(), e.getScore());
            }
        }
    }

    public int getDomain() {
        return domain;
    }

    public int getClusterID() {
        return cluster == null ? -1 : cluster.id;
    }

    /**
     * @return the content this node contains
     */
    public String getContent() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return set of unique terms
     */
    public Set<String> getTerms() {
        throw new UnsupportedOperationException();
    }

    public Cluster majority() {
        if (countNearestNeighbors() == Cluster.K) {
            if (edge[0].node != null && edge[0].node.getCluster() != null) {
                Cluster c = edge[0].node.getCluster();
                if ((edge[1].node != null && edge[1].node.cluster == c)
                        || (edge[2].node != null && edge[2].node.cluster == c)) {
                    return c;
                }
            } else if (edge[1].node != null && edge[2].node != null
                    && edge[1].node.getCluster() == edge[2].node.getCluster()) {
                return edge[1].node.getCluster();
            }
        } else if (countNearestNeighbors() > Cluster.BREAKTIE) {
            if (edge[0].node != null && edge[1].node != null
                    && edge[0].node.getCluster() == edge[1].node.getCluster()) {
                return edge[0].node.getCluster();
            }
        }
        return null;
    }

    /**
     * @param otherNode
     * @return true when this node has otherNode as a direct nearest neighbor.
     */
    public boolean linkedTo(Node otherNode) {
        for (int i = 0; i < countNearestNeighbors(); i++) {
            if (getNearestNeighbor(i).getNode() == otherNode) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return the number of assigned nearest neighbors
     */
    public int countNearestNeighbors() {
        return edges;
    }

    /**
     * @return the similarity score of the least similar nearest neighbor, or 0
     * if there are less than K nearest neighbors.
     */
    public double getLowestScore() {
        return edge[Cluster.K - 1] == null ? 0 : edge[Cluster.K - 1].score;
    }

    /**
     * @return the least similar nearest neighbor, or null if the node has no
     * nearest neighbors.
     */
    public Node getWeakestNearestNeighbor() {
        return edges == 0 ? null : edge[edges - 1].node;
    }
    
    public int countNonNullNeighbors() {
        int count = 0;
        for (int i = 0; i < edges; i++)
            if (edge[i].node != null)
                count++;
        return count;
    }
    
    /**
     * @return an Iterator/Iterable over the edges to this nodes nearest neighbors
     */
    public EdgeIterator<Node> iterator() {
        return new EdgeIterator(this);
    }

    public int hashCode() {
        return MathTools.hashCode(id);
    }

    public boolean equals(Object o) {
        return (o instanceof Node) && ((Node) o).id == id;
    }

    /**
     * @return true when the node is assigned to a cluster
     */
    public boolean isClustered() {
        return cluster != null;
    }

    /**
     * @return true when the node is assigned to the core of a cluster
     */
    public boolean isClusterCoreNode() {
        return cluster != null && cluster.getCore().contains(this);
    }

    /**
     * Assign the node to cluster c. When the node was previously assigned to a
     * different cluster it is removed as a member for this cluster.
     *
     * @param c
     */
    public void setCluster(Cluster c) {
        if (cluster != c) {
            if ((cluster != null && cluster.watch) || watch || (c != null && c.watch)) {
                log.info("setClusterPre %s newcluster %s", this.toClusterString(), c == null ? -1 : c.getID());
            }
            if (this.cluster != null) {
                cluster.remove(this);
            }
            this.cluster = c;
            if (c != null) {
                if ((c != null && c.watch) || watch) {
                    log.info("setClusterPost url %s", this.toClusterString());
                }
                c.addNode(this);
            } else if (watch) {
                log.info("unCluster %s", this.toClusterString());
            }
        }
    }

    /**
     * @return the currently assigned cluster or null if not assigned
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * @return a comma separated string representation of the ids of this nodes
     * nearest neighbors
     */
    public String getNearestNeighborIds() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < countNearestNeighbors(); i++) {
            Node url = getNearestNeighbor(i).getNode();
            if (url != null) {
                sb.append(",").append(getNearestNeighbor(i).getNode().getID());
            } else {
                sb.append(",").append(-1);
            }
        }
        return (countNearestNeighbors() > 0) ? sb.deleteCharAt(0).toString() : "";
    }

    /**
     * @return a comma separated string representation of the similarity scores
     * of this nodes nearest neighbors
     */
    public String getNearestNeighborScores() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < countNearestNeighbors(); i++) {
            sb.append(",").append(getNearestNeighbor(i).getScore());
        }
        return (countNearestNeighbors() > 0) ? sb.deleteCharAt(0).toString() : "";
    }

    public long getCreationTime() {
        return creationtime;
    }

    @Override
    public int compareTo(Node o) {
        return getLowestScore() < o.getLowestScore() ? -1 : 1;
    }

    private class Vertex implements Comparable<Vertex> {

        Node url;
        ArrayList<Node> edges;
        ArrayList<Node> path;
        int pathid;
        int steps;
        int walked = 0;

        public Vertex(Node start, Node url, int pathid, int steps) {
            this.url = url;
            this.pathid = pathid;
            path = new ArrayList();
            path.add(start);
            path.add(url);
            this.steps = steps;
            edges = url.getNextBidirectionalEdges(start.getCluster());
            edges.remove(start);
        }

        public Vertex(Vertex v, Node url) {
            this.url = url;
            path = (ArrayList) v.path.clone();
            path.add(url);
            this.steps = v.steps + 1;
            edges = url.getNextBidirectionalEdges(path.get(0).getCluster());
            edges.removeAll(path);
        }

        @Override
        public boolean equals(Object o) {
            return url == ((Vertex) o).url;
        }

        public int hashCode() {
            return url.hashCode();
        }

        public Node next() {
            if (hasNext()) {
                return edges.get(walked++);
            }
            return null;
        }

        public boolean hasNext() {
            return walked < edges.size();
        }

        @Override
        public int compareTo(Vertex o) {
            return steps - o.steps;
        }
    }

    public boolean possibleCoreNode() {
        if (backlinks != null) {
            int count = 0;
            for (int i = 0; i < edges; i++) {
                if (edge[i].node != null && backlinks.contains(edge[i].node)) {
                    if (++count > Cluster.BREAKTIE) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     *
     * @return A 2-degenerate core this node is a member of, or an empty set if
     * no such core exists.
     */
    public FHashSet<Node> get2DegenerateCore() {
        ArrayList<Node> biconnected = getNextBidirectionalEdges(getCluster());
        PriorityQueue<Vertex> queue = new PriorityQueue();
        HashMap<Long, Vertex> visited = new HashMap();
        for (int i = 0; i < biconnected.size(); i++) {
            Node u = biconnected.get(i);
            Vertex v = new Vertex(this, u, i, 1);
            if (v.hasNext()) {
                queue.add(v);
                visited.put(u.getID(), v);
            }
        }
        FHashSet<Node> solution = new FHashSet();
        while (queue.size() > 0) {
            Vertex first = queue.poll();
            while (first.hasNext()) {
                Node u = first.next();
                Vertex existing = visited.get(u.getID());
                if (existing != null) {
                    if (existing.pathid != first.pathid && existing.url.linkedTo(first.url)) {
                        for (Node e : existing.path) {
                            solution.add(e);
                        }
                        for (Node e : first.path) {
                            solution.add(e);
                        }
                        PriorityQueue newqueue = new PriorityQueue();
                        if (queue.size() > 2) {
                            for (Vertex v : queue) {
                                if (v.pathid != first.pathid && v.pathid != existing.pathid) {
                                    newqueue.add(v);
                                }
                            }
                        }
                        queue = newqueue;
                        break;
                    }
                } else {
                    Vertex v = new Vertex(first, u);
                    queue.add(v);
                    visited.put(u.getID(), v);
                }
            }
        }
        return solution;
    }

    public boolean possibleCoreNode2() {
        int count = 0;
        for (int e = 0; e < edges; e++) {
            Node nn = edge[e].node;
            if (nn != null && nn.possibleCoreNode()) {
                count++;
            }
        }
        return count > Cluster.BREAKTIE;
    }

    private ArrayList<Node> getNextBidirectionalEdges(Cluster cluster) {
        ArrayList<Node> result = new ArrayList();
        for (int e = 0; e < edges; e++) {
            Node dest = ((Node) edge[e].getNode());
            if (dest != null && dest.linkedTo(this) && (dest.getCluster() == cluster || !dest.isClusterCoreNode())) {
                result.add(dest);
            }
        }
        return result;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %d ", edges, id));
        return sb.toString();
    }

    public String toClusterString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("%d [%d]: ", id, cluster == null ? -1 : cluster.getID()));
        for (Edge e : new EdgeIterator<Node>(this)) {
            if (e.getNode() == null) {
                sb.append(sprintf("x [-1] "));
            } else {
                sb.append(sprintf("%d [%d] ", e.getNode().id, e.getNode().cluster == null ? -1 : e.getNode().cluster.getID()));
            }
        }
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %d", edges, id));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }

    public String toStringEdges2() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url %d [%d]:", id, getClusterID()));
        for (int i = 0; i < edges; i++) {
            sb.append(sprintf(" %d [%d]", edge[i].node.getID(), edge[i].getNode().getClusterID()));
        }
        return sb.toString();
    }
}
