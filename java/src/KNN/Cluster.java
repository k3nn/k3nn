package KNN;

import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class Cluster<N extends Node> {

    public static final Log log = new Log(Cluster.class);
    public static final int K = 3;
    public static final int BREAKTIE = K / 2;
    private Stream stream;
    private static HashSet<Integer> watchlist = new HashSet(Arrays.asList());
    public final boolean watch;
    final int id;
    private ArrayList<N> nodes = new ArrayList();
    private HashSet<N> base = new HashSet();

    Cluster(Stream stream, int id, Collection<Node> base) {
        this.id = id;
        this.stream = stream;
        setBase(base);
        for (Node u : base) {
            u.setCluster(this);
        }
        watch = false; //watch(this);
        if (watch) {
            log.info("create %d", id);
            for (Node u : base) {
                log.info("base %d %s %s", u.getID(), u.getNN(), u.getScore());
            }
        }
    }

    Cluster(Stream stream, int id) {
        this.stream = stream;
        this.id = id;
        watch = watch(this);
        if (watch) {
            log.info("create %d", id);
        }
    }
    
    /**
    * Only for debug purposes
    */
    public static Cluster createCluster() {
        return new Cluster(null, 0);
    }

    public Cluster<N> shallowClone() {
        Cluster clone = new Cluster(null, id);
        clone.setBase(base);
        clone.nodes = new ArrayList(nodes);
        return clone;
    }

    public Cluster<N> shallowClone(N last) {
        Cluster clone = new Cluster(null, id);
        clone.setBase(base);
        clone.nodes = new ArrayList();
        for (N node : nodes)
            if (node.getID() != last.getID())
                clone.nodes.add(node);
        clone.nodes.add(last);
        return clone;
    }

    private static boolean watch(Cluster c) {
        return (watchlist.contains(c.getID()));
    }

    public void setBase(Collection<? extends Node> base) {
        if (base.size() == 2) {
            //log.crash();
        }
        this.base = new HashSet(base);
    }

    public boolean recheckBase() {
        HashSet<Node> base = getBase(nodes);
        if (watch) {
            log.info("reBase %s", base);
        }
        if (base.isEmpty()) {
            //stream.changedclusters.add(id);
            this.base.clear();
            ArrayList<N> urls = new ArrayList(this.nodes);
            // only reset for non-shallow clones
            if (stream != null) {
                for (Node u : urls) {
                    u.setCluster(null);
                }
                stream.remove(this);
            }
            if (watch) {
                log.info("reBase remove cluster %s", base);
            }
            return false;
        } else {
            if (!base.equals(this.base)) {
                //stream.changedclusters.add(id);
                setBase(base);
            }
            return true;
        }
    }

    public int getID() {
        return id;
    }

    public void addUrl(N node) {
        if (node.watch) {
            log.info("Add Url %d to cluster %d", node.getID(), getID());
        }
        if (nodes.contains(node)) {
            log.info("Cluster %d", getID());
            for (Node u : nodes) {
                log.info("contains %d", u.getID());
            }
            log.fatal("duplicate cluster %d url %d time %d", id, node.getID(), node.creationtime);
        }
        //stream.changedclusters.add(id);
        nodes.add(node);
        if (watch) {
            log.info("addUrl cluster %d url %d", id, node.getID());
        }
    }

    public void addUrlDontCheck(N node) {
        if (!nodes.contains(node)) {
            if (watch) {
                log.info("addUrlDontCheck cluster %d url %d", id, node.getID());
            }
            //stream.changedclusters.add(id);
            nodes.add(node);
        }
    }

    public ArrayList<N> getNodes() {
        return nodes;
    }

    public HashSet<N> getBase() {
        return base;
    }

    public HashSet<Node> linkedTo(HashSet<Node> set) {
        HashSet<Node> result = new HashSet();
        for (Node u : nodes) {
            if (u.linkedTo(set)) {
                result.add(u);
            }
        }
        return result;
    }

    public HashSet<Node> linkedToNoBase(HashSet<Node> set) {
        HashSet<Node> result = new HashSet();
        for (Node u : nodes) {
            if (!base.contains(u) && u.linkedTo(set)) {
                result.add(u);
            }
        }
        return result;
    }

    public void remove(Node node) {
        if (watch || node.watch) {
            log.info("remove cluster %d url %d", id, node.getID());
        }
        //stream.changedclusters.add(id);
        getNodes().remove(node);
    }

    static class Node2<Url> extends HashMap<Url, HashSet<Url>> {

        void addEdge(Url node, Url edge) {
            HashSet<Url> list = get(node);
            if (list == null) {
                list = new HashSet();
                put(node, list);
            }
            list.add(edge);
        }
    }

    public static final HashSet<Node> emptylist = new HashSet();

    public static HashSet<Node> getBase(Collection<? extends Node> base) {
        for (Node u : base) {
            int count = 0;
            for (int e = 0; e < u.edges; e++) {
                Edge edge = u.getNN(e);
                Node to = edge.getNode();
                if (to != null) {
                    if (to.getID() > u.getID() && base.contains(to) && to.linkedTo(u)) {
                        if (++count > Cluster.BREAKTIE) {
                            HashSet<Node> base1 = u.getBase();
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

//    public double getAvgBaseScore() {
//        double basescore = 0;
//        int count = 0;
//        for (Node u : getBase()) {
//            for (int i = 0; i < u.getEdges(); i++) {
//                Edge e = u.getNN(i);
//                if (getBase().contains(e.node)) {
//                    basescore += e.getScore();
//                    count++;
//                }
//            }
//        }
//        basescore /= count;
//        return basescore;
//    }
    public int size() {
        return nodes.size();
    }

    public double getAvgBaseScore() {
        double basescore = 0;
        int count = 0;
        for (N u : getBase()) {
            for (int i = 0; i < u.getEdges(); i++) {
                Edge e = u.getNN(i);
                if (getBase().contains(e.node)) {
                    basescore += e.getScore();
                    count++;
                }
            }
        }
        basescore /= count;
        return basescore;
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
    public Cluster stripCluster() {
        HashSet<N> urls = new HashSet();
        HashSet<N> newurls = getBase();
        while (newurls.size() > 0) {
            urls.addAll(newurls);
            HashSet<N> nextbatch = new HashSet();
            for (N u : newurls) {
                for (int e = 0; e < u.getEdges(); e++) {
                    N l = (N)u.getNN(e).getNode();
                    if (l != null && !urls.contains(l)) {
                        nextbatch.add(l);
                    }
                }
            }
            newurls = nextbatch;
        }
        Cluster<N> clone = shallowClone();
        
        HashSet<Node> remove = new HashSet(getNodes());
        remove.removeAll(urls);
        for (Node u : remove) {
            clone.remove(u);
        }
        return clone;
    }
    
    
    @Override
    public String toString() {
        //return Integer.toString(id);
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("\ncluster %d [%d] ", getID(), nodes.size()));
        for (Node node : nodes) {
            sb.append("\n node ").append(node.toString());
        }
        for (Node node : base) {
            sb.append("\n base ").append(node.toString());
        }
        return sb.toString();
    }

    public String evalall() {
        StringBuilder sb = new StringBuilder();
        sb.append(eval()).append("\n-----\n");
        HashSet<Node> covered = new HashSet(getBase());
        HashSet<Node> todo = new HashSet(getNodes());
        todo.removeAll(covered);
        int size = nodes.size();
        while (size > todo.size()) {
            size = todo.size();
            Iterator<Node> iter = todo.iterator();
            while (iter.hasNext()) {
                Node u = iter.next();
                int count = 0;
                for (int i = 0; i < u.edges; i++) {
                    Node nn = u.getNN(i).getNode();
                    if (nn != null && covered.contains(nn)) {
                        count++;
                    }
                }
                if (count > Cluster.BREAKTIE) {
                    iter.remove();
                    covered.add(u);
                    sb.append(u.getID()).append(": ");
                    addEval(sb, u);
                }
            }
        }
        if (todo.size() > 0) {
            sb.append("\n---- misplaced ---\n");
            for (Node u : todo) {
                sb.append(u.getID()).append(": ");
                addEval(sb, u);
            }
        }
        return sb.toString();
    }

    public String eval() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("CLUSTER %d size=%d\n", id, nodes.size()));
        for (Node u : base) {
            sb.append(u.getID()).append(":");
            addEval(sb, u);
        }
        return sb.toString();
    }

    private void addEval(StringBuilder sb, Node u) {
        LOOP:
        for (Edge edge : new EdgeIterator(u)) {
            Node l = edge.getNode();
            if (l != null) {
                sb.append(l.getID()).append(" ");
            } else {
                sb.append("x ");
            }
        }
        for (Edge edge : new EdgeIterator(u)) {
            sb.append(edge.score).append(" ");
        }
        sb.append("\n");
    }
}
