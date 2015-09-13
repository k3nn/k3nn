package io.github.k3nn;

import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.fcollection.FHashMapLongInt;
import io.github.htools.lib.Log;
import io.github.htools.fcollection.FHashSet;
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

    public void addFast(N node, Collection<String> features) {
        ArrayMapDouble<N> map = iinodes.addGetList(node, features);
        iinodes.getNodes().put(node.getID(), node);

        for (Double2ObjectMap.Entry<N> entry : map) {
            if (entry.getDoubleKey() > node.getLowestScore()) {
                node.add(new Edge(entry.getValue(), entry.getDoubleKey()));
            }
            if (entry.getDoubleKey() > entry.getValue().getLowestScore()) {
                entry.getValue().add(new Edge(node, entry.getDoubleKey()));
            }
        }
    }

    public void reconstructClusters(FHashMapLongInt oldclusters) {
        clusters = new HashMap();
        for (N node : iinodes.getNodes().values()) {
            node.setCluster(null);
        }
        for (N node : iinodes.getNodes().values()) {
            if (node.getCluster() == null) {
                FHashSet<N> coreNodes = (FHashSet<N>)node.get2DegenerateCore();
                if (coreNodes.size() >= Cluster.K) {
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
                    Cluster<N> c = Cluster.createCoreCluster(this, clusterid, coreNodes);
                    assignNodes(c);
                    this.clusters.put(clusterid, c);
                }
            }
        }
    }

    public void assignNodes(Cluster<N> c) {
        FHashSet<N> newnodes = new FHashSet(c.getCore());
        HashMapInt<N> votes = new HashMapInt();
        FHashSet<Node> linked = new FHashSet();
        while (newnodes.size() > 0) {
            for (N node : newnodes) {
                for (int i = 0; i < node.edges; i++) {
                    if (node.edge[i].node != null) {
                        linked.add(node.edge[i].node);
                    }
                }
            }
            for (Node u : newnodes) {
                if (u.backlinks != null) {
                    for (Node b : u.backlinks) {
                        if (!b.isClustered() && !c.getNodes().contains(b)) {
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
                    if (u.getCluster() != c && linked.contains(u)) {
                        u.setCluster(c);
                        newnodes.add(u);
                    }
                }
            }
        }
    }
}
