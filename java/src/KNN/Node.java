package KNN;

import static KNN.Stream.log;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.MathTools;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

/**
 * A Node that is used for clustering. A node represents a piece of information, 
 * e.g. a sentence, which is connected to its nearest neighbors based on similarity 
 * in features (e.g. words, domain, creation time). The most basic implementation 
 * of a Node is stripped from everything except the clustering information, to minimize
 * memory used. As such a cluster of Node will supply only internal Node ID's, and
 * information about assigned nearest neighbors and cluster. Note that this class 
 * cannot be clustered since number of features is missing. Other subclasses of Node
 * have attributes to report more elaborately, e.g. NodeM holds the number of terms
 * to allow comparing similarity, NodeT contains also the title. 
 * @author jeroen
 */
public abstract class Node implements Comparable<Node> {

    public static Log log = new Log(Node.class);
    public static HashSet<Integer> watchlist = new HashSet(Arrays.asList()); // for debugging
    public final boolean watch; // used for debugging
    private long id;       
    protected Edge[] edge = new Edge[Cluster.K]; // links to nearest neighbors
    protected int edges = 0; // number of edges used
    HashSet<Node> backlinks; // nodes that have this node as nearest neighbor
    public int domain = -1;
    protected long creationtime;
    protected Cluster cluster = null;

    public Node(long id) {
        this.id = id;
        watch = watch(this); // for debug purposes
    }

    public Node(long id, int domain, long creationtime) {
        this(id);
        this.domain = domain;
        this.creationtime = creationtime;
    }

    private static boolean watch(Node url) {
        return watchlist.contains(url.getID());
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
    public Edge getNN(int i) {
        return edge[i];
    }

    private void addBackLink(Node u) {
        if (backlinks == null) {
            backlinks = new HashSet();
        }
        backlinks.add(u);
    }

    private void removeBackLink(Node u) {
        backlinks.remove(u);
    }    
    
    public HashSet<Node> getBacklinks() {
        return backlinks;
    }
    
    /**
     * add an Edge if the score is higher than the weakest NN, the NNs remain sorted
     * @param e 
     */
    public void add(Edge e) {
        if (edges == 0) {
            edge[edges++] = e;
            if (e.getNode() != null) {
                ((Node)e.getNode()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d 0 %d %f", getID(), e.getNode() == null ? -1 : e.getNode().getID(), e.getScore());
            }
        } else if (edge[edges - 1].getScore() < e.getScore()) {
            if (edges == Cluster.K && edge[Cluster.K - 1].getNode() != null) {
                ((Node)edge[Cluster.K - 1].getNode()).removeBackLink(this);
            }
            for (int pos = 0; pos < edges; pos++) {
                if (edge[pos].getScore() < e.getScore()) {
                    for (int i = Math.min(Cluster.K - 1, edges); i > pos; i--) {
                        edge[i] = edge[i - 1];
                    }
                    edge[pos] = e;
                    if (e.getNode() != null) {
                        ((Node)e.getNode()).addBackLink(this);
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
                ((Node)e.getNode()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d %d %d %f", getID(), edges - 1, e.getNode() == null ? -1 : e.getNode().getID(), e.getScore());
            }
        }
    }

    public int getDomain() {
        return domain;
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
    public HashSet<String> getTerms() {
        throw new UnsupportedOperationException();
    }

    public Cluster majority() {
        Cluster cluster = null;
        if (getEdges() > Cluster.BREAKTIE + 1) {
            LOOP:
            for (int i = Cluster.K - Cluster.BREAKTIE - 1; i >= 0; i--) {
                Edge edge = getNN(i);
                if (edge == null) {
                    log.info("majority url %d edges %d edge %d NULL", id, getEdges(), i);
                }
                Node nn = edge.getNode();
                if (nn == null) {
                    log.info("majority url %d edges %d edge %d nn NULL", id, getEdges(), i);
                } else {
                    cluster = nn.getCluster();
                    if (cluster != null) {
                        int equal = Cluster.BREAKTIE - i;
                        for (int j = i + 1; j < Cluster.K; j++) {
                            Node nnj = getNN(j).getNode();
                            if (nnj == null || cluster != nnj.getCluster()) {
                                if (--equal < 0) {
                                    cluster = null;
                                    continue LOOP;
                                }
                            }
                        }
                        break LOOP;
                    }
                }
            }
        } else if (getEdges() > Cluster.BREAKTIE) {
            Node nn0 = getNN(0).getNode();
            if (nn0 != null) {
                cluster = nn0.getCluster();
                if (cluster != null) {
                    for (int i = 1; i <= Cluster.BREAKTIE; i++) {
                        Node nn = getNN(i).getNode();
                        if (nn != null && cluster != nn.getCluster()) {
                            cluster = null;
                            break;
                        }
                    }
                }
            }
        }
        return cluster;
    }

    public boolean linkedTo(Node urlto) {
        //Profiler.startTime("linkedTo");
        for (int i = 0; i < getEdges(); i++) {
            if (getNN(i).getNode() == urlto) {
                //Profiler.addTime("linkedTo");
                return true;
            }
        }
        //Profiler.addTime("linkedTo");
        return false;
    }

    public <U extends Node> boolean linkedTo(HashSet<U> urls) {
        for (int i = 0; i < getEdges(); i++) {
            Node url = getNN(i).getNode();
            if (url != null && urls.contains(url)) {
                return true;
            }
        }
        return false;
    }

    public boolean lostClusterMajority() {
        return cluster != majority();
    }

    public int getEdges() {
        return edges;
    }

    public Double getLowestScore() {
        return edge[Cluster.K - 1] == null ? 0 : edge[Cluster.K - 1].score;
    }

    public Node getLowestNN() {
        return edges == 0 ? null : edge[edges - 1].node;
    }

    public Double getAvgScore() {
        double score = 0;
        int count = 0;
        for (int i = 0; i < edges; i++) {
            score += edge[i].score;
        }
        return score / edges;
    }

    public int hashCode() {
        return MathTools.hashCode(id);
    }

    public boolean equals(Object o) {
        return (o instanceof Node) && ((Node)o).id == id;
    }

    public boolean isClustered() {
        return cluster != null;
    }

    public boolean isBaseNode() {
        if (cluster != null)
            return cluster.getBase().contains(this);
        return false;
    }

    public void setCluster(Cluster c) {
        if (cluster != c) {
//            if (this.cluster != null && listenclusters != null) {
//                listenclusters.remove(this.cluster.getID());
//            }
            if (watch) {
                log.info("setCluster %s oldcluster %s", this.toClusterString(), cluster);
            }
            if (this.cluster != null) {
                cluster.remove(this);
            }
            this.cluster = c;
            if (c != null) {
                if (watch) {
                    log.info("setCluster url %s", this.toClusterString());
                }
                c.addUrl(this);
            } else if (watch) {
                log.info("unCluster %s", this.toClusterString());
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public ArrayList<Node> getNextBidirectionalEdges() {
        ArrayList<Node> result = new ArrayList();
        for (int e = 0; e < edges; e++) {
            Node dest = ((Node)edge[e].getNode());
            if (dest != null && dest.linkedTo(this)) {
                result.add(dest);
            }
        }
        return result;
    }

    public String getNN() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getEdges(); i++) {
            Node url = getNN(i).getNode();
            if (url != null) {
                sb.append(",").append(getNN(i).getNode().getID());
            } else {
                sb.append(",").append(-1);
            }
        }
        return (getEdges() > 0) ? sb.deleteCharAt(0).toString() : "";
    }

    public String getScore() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getEdges(); i++) {
            sb.append(",").append(getNN(i).getScore());
        }
        return (getEdges() > 0) ? sb.deleteCharAt(0).toString() : "";
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
            edges = url.getNextBidirectionalEdges();
            edges.remove(start);
        }

        public Vertex(Vertex v, Node url) {
            this.url = url;
            path = (ArrayList) v.path.clone();
            path.add(url);
            this.steps = v.steps + 1;
            edges = url.getNextBidirectionalEdges();
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

    public HashSet<Node> getBase() {
        ArrayList<Node> biconnected = getNextBidirectionalEdges();
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
        HashSet<Node> solution = new HashSet();
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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %d ", edges, id));
        //for (int i = 0; i < edges; i++) {
        //    sb.append(nn[i].toString());
        //}
        return sb.toString();
    }

    public String toClusterString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("%d [%d]: ", id, cluster == null ? -1 : cluster.getID()));
        for (Edge e : new EdgeIterator(this)) {
            if (e.getNode() == null) {
                sb.append(sprintf("x [-1] "));
            } else {
                sb.append(sprintf("%d [%d] ", e.getNode().id, e.getNode().cluster == null ? -1 : e.getNode().cluster.getID()));
            }
        }
        //for (int i = 0; i < edges; i++) {
        //    sb.append(nn[i].toString());
        //}
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
}
