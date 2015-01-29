package KNN2;

import KNN.Cluster;
import KNN.Edge;
import KNN.EdgeIterator;
import static KNN.Stream.log;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.lib.Profiler;
import io.github.repir.tools.type.Tuple2Comparable;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

/**
 *
 * @author jeroen
 */
public class Stream<U extends Url> extends KNN.Stream<U> {

    public void add(U url, Collection<String> features) {
        //log.info("add %d", url.getID());
        if (Url.modifiedurls.size() > 0) {
            Url.modifiedurls = new HashSet();
        }
        ArrayMap<Tuple2Comparable<Integer, Integer>, U> map = iiurls.candidateUrlsCount(features);
        HashSet<U> changedUrls = new HashSet();
        HashSet<Cluster> recheckcluster = new HashSet();
        double sqrtsize = Math.sqrt(((UrlM) url).countFeatures());
        double lowestscore = 0;

        for (Map.Entry<Tuple2Comparable<Integer, Integer>, U> entry : map.descending()) {
            U u = entry.getValue();
            if (u.getDomain() != url.getDomain()) {
                double score = Score.timeliness(url.getCreationTime(), u.getCreationTime());
                //log.trace("timeliness %f %d %d", score, u.countFeatures(), url.countFeatures());
                if (score > lowestscore || score > u.getLowestScore()) {
                    score *= entry.getKey().value1 / (sqrtsize * Math.sqrt(((UrlM) u).countFeatures()));
                    if (score > lowestscore || score > u.getLowestScore()) {
                        getBest3(url, u, score, changedUrls);
                        lowestscore = url.getLowestScore();
                    }
                }
            }
        }
        //log.info("url %d cluster %d", url.getID(), url.isClustered() ? url.getCluster().getID() : -1);
        //log.info("changedUrls %d %s", changedUrls.size(), changedUrls);
        HashSet<U> changed2 = new HashSet();
        while (changedUrls.size() > 0) {
            HashSet<U> newchanged = new HashSet();
            for (U u : changedUrls) {
                if (u.isClustered()) {
                    Cluster<U> c = u.getCluster();
                    //log.info("Changed %d isBase %b", u.getID(), c.getBase().contains(u));
                    //if (u.lostClusterMajority()) {
                        if (c.getBase().contains(u)) {
                            recheckcluster.add(c);
                        } else {
                            u.setCluster(null);
                            if (((Url) u).backlinks != null) {
                                for (Url b : ((Url) u).backlinks) {
                                    if (b.isClustered() && b.getCluster() == c) {
                                        newchanged.add((U) b);
                                    }
                                }
                            }
                            changed2.add(u);
                        }
                    //}
                }
            }
            changedUrls = newchanged;
        }
        //log.info("changedUrls %d %s", changed2.size(), changed2);
        for (Cluster<U> c : recheckcluster) {
            HashSet<U> base = findBase(c);
            //if (c.watch) {
                //log.info("recheckcluster %d baseempty %b", c.getID(), base.isEmpty());
            //}
            if (base.isEmpty()) {
                if (changed2.contains(watch)) {
                    log.info("base2 %b %s", base.isEmpty(), base);
                }
                ArrayList<U> urls = new ArrayList(c.getUrls());
                for (U u : urls) {
                    u.setCluster(null);
                }
                remove(c);
                if (c.watch) {
                    log.info("remove rechecked cluster %d", c.getID());
                }
            } else if (!c.getBase().equals(base)) {
                c.setBase(base);
                ArrayList<U> list = new ArrayList(c.getUrls());
                list.removeAll(base);
                for (Url u : list) {
                    u.setCluster(null);
                }
                this.assignNodes(c);
                changed2.removeAll(c.getUrls());
            }
        }
        if (!url.isClustered()) {
            changed2.add(url);
        }
        addMajority(changed2);
        if (!url.isClustered()) {
            clusterMajorityBase(url);
        } else if (url.backlinks != null) {
            assignNodes(url.getCluster());
        }
        iiurls.add(url, features);

        if (Url.modifiedurls.size() > 0) {
            listener.urlChanged(url, Url.modifiedurls);
        }
    }

    @Override
    public void addMajority(HashSet<? extends KNN.Url> urls) {
        int size = 0;
        while (size != urls.size()) {
            size = urls.size();
            HashSet<KNN.Url> newset = new HashSet();
            for (KNN.Url u : urls) {
                Cluster c = u.majority();
                if (u.watch)
                    log.info("addMajority %d %s %s", u.getID(), u.getCluster(), c);
                if (c != null && c != u.getCluster()) {
                    //log.info("majority %d %d", u.getID(), c.getID());
                    u.setCluster(c);
                    if (((Url)u).backlinks != null) {
                        for (Url b : ((Url)u).backlinks)
                            if (!b.isClustered())
                               newset.add(b);
                    }
                } else {
                    newset.add(u);
                }
            }
            urls = newset;
        }
    }
    
    public void showWatchUrls() {
        for (int id : Url.watchlist) {
            Url u = this.urls.get(id);
            if (u != null) {
                StringBuilder sb = new StringBuilder();
                for (Edge<U> e : new EdgeIterator(u)) {
                    if (e.getUrl() != null) {
                        Cluster cluster = e.getUrl().getCluster();
                        sb.append(sprintf("%d %d ", e.getUrl().getID(), cluster == null ? -1 : cluster.getID()));
                    }
                }
                //log.info("Watch %d %d %s", id, u.getCluster() == null ? -1 : u.getCluster().getID(), sb);
            }
        }
    }

    public HashSet<U> findBase(Cluster<U> c) {
        for (U u : c.getBase()) {
            HashSet<U> base = getBase2(u);
            if (base.size() > Cluster.BREAKTIE) {
                return base;
            }
        }
        return new HashSet<U>();
    }

    public void assignNodes(Cluster<U> c) {
        HashSet<U> newnodes = new HashSet(c.getUrls());
        HashMapInt<U> votes = new HashMapInt();
        while (newnodes.size() > 0) {
            for (Url u : newnodes) {
                if (u.backlinks != null) {
                    for (Url b : u.backlinks) {
                        if (!b.isClustered() && !c.getUrls().contains(b)) {
                            votes.add((U)b, 1);
                        }
                    }
                }
            }
            newnodes = new HashSet();
            Iterator<Map.Entry<U, Integer>> iter = votes.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<U, Integer> next = iter.next();
                U u = next.getKey();
                if (next.getValue() > Cluster.BREAKTIE) {
                    iter.remove();
                    if (!u.isClustered()) {
                       next.getKey().setCluster(c);
                       newnodes.add(next.getKey());
                    }
                }
            }
        }
    }

    public void clusterMajorityBase(U start) {
        HashSet<U> base = getBase2(start);
        if (base.size() > Cluster.BREAKTIE) {
            Cluster<U> newcluster = createCluster(base);
            assignNodes(newcluster);
        }
    }

    class Vertex implements Comparable<Vertex> {

        Url url;
        ArrayList<Url> edges;
        ArrayList<Url> path;
        int pathid;
        int steps;
        int walked = 0;

        public Vertex(Url start, Url url, int pathid, int steps) {
            this.url = url;
            this.pathid = pathid;
            path = new ArrayList();
            path.add(start);
            path.add(url);
            this.steps = steps;
            edges = url.getNextBidirectionalEdges();
            edges.remove(start);
        }

        public Vertex(Vertex v, Url url) {
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

        public Url next() {
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

    public HashSet<U> getBase2(U start) {
        ArrayList<Url> biconnected = start.getNextBidirectionalEdges();
        //log.info("getBase2 %d biconnected %s", start.getID(), biconnected);
        PriorityQueue<Vertex> queue = new PriorityQueue();
        HashMap<Integer, Vertex> visited = new HashMap();
        for (int i = 0; i < biconnected.size(); i++) {
            Url u = biconnected.get(i);
            Vertex v = new Vertex(start, u, i, 1);
            if (v.hasNext()) {
                queue.add(v);
                visited.put(u.getID(), v);
            }
        }
        HashSet<U> solution = new HashSet();
        while (queue.size() > 0) {
            Vertex first = queue.poll();
            while (first.hasNext()) {
                Url u = first.next();
                Vertex existing = visited.get(u.getID());
                if (existing != null) {
                    //log.info("getBase2 %d %d %b", first.url.getID(), existing.url.getID(), existing.url.linkedTo(first.url));
                    if (existing.pathid != first.pathid && existing.url.linkedTo(first.url)) {
                        for (Url e : existing.path) {
                            solution.add((U) e);
                        }
                        for (Url e : first.path) {
                            solution.add((U) e);
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

    public void getBest3(U newurl,
            U existing,
            double score,
            HashSet<U> changes) {
        //log.info("getBest1 url %s cluster %s", url.url, u);
        if (score > newurl.getLowestScore()) {
            newurl.add(createEdge(newurl, existing, score));
        }
        if (newurl.watch) {
            log.info("getBest3 url %d %s %s", newurl.getID(), newurl.getNN(), newurl.getScore());
        }
        if (score > existing.getLowestScore()) {
            Edge<U> e = createEdge(existing, newurl, score);
            changes.add(existing);
            if (existing.watch && existing.getEdges() == Cluster.K) {
                KNN.Url nn = existing.getLowestNN();
                //log.info("steal url %d nn %d score %f", existing.getID(), nn.getID(), existing.getLowestScore());
            }
            existing.add(e);
            if (existing.watch && existing.getEdges() == Cluster.K) {
                //log.info("steal url %d %s %s", existing.getID(), existing.getNN(), existing.getScore());
            }
        }
    }

    public int unclustered() {
        int count = 0;
        for (U u : urls.values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }

    public void eval() {
        int sec = (int) (System.currentTimeMillis() - starttime) / 1000;
        log.info("urls %d", this.urls.size());
        log.info("clusters %d", getClusters().size());
        log.info("unclusterd %d", unclustered());
        log.printf("time past %d:%ds", sec / 60, sec % 60);
        HashMapInt<Integer> dist = new HashMapInt();
        for (Cluster cl : getClusters()) {
            dist.add(cl.size(), 1);
        }
        TreeMap<Integer, Integer> sorted = new TreeMap(dist);
        log.info("sorted %s", sorted);

        Profiler.reportProfile();
        log.exit();
    }
}
