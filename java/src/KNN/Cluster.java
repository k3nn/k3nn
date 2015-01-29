package KNN;

import io.github.repir.tools.collection.HashMapDouble;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.type.Tuple2;
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
public class Cluster<U extends Url> {

    public static final Log log = new Log(Cluster.class);
    public static final int K = 3;
    public static final int BREAKTIE = K / 2;
    private Stream stream;
    private static HashSet<Integer> watchlist = new HashSet(Arrays.asList());
    public final boolean watch;
    final int id;
    private ArrayList<U> urls = new ArrayList();
    private HashSet<U> base = new HashSet();

    Cluster(Stream stream, int id, Collection<U> base) {
        this.id = id;
        this.stream = stream;
        setBase(base);
        for (Url u : base) {
            u.setCluster(this);
        }
        watch = false; //watch(this);
        if (watch) {
            log.info("create %d", id);
            for (Url u : base) {
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

    private static boolean watch(Cluster c) {
        return (watchlist.contains(c.getID()));
    }

    public void setBase(Collection<? extends Url> base) {
        if (base.size() == 2) {
            //log.crash();
        }
        this.base = new HashSet(base);
    }

    public boolean recheckBase() {
        HashSet<Url> base = getBase(urls);
        if (watch) {
            log.info("reBase %s", base);
        }
        if (base.isEmpty()) {
            stream.changedclusters.add(id);
            this.base.clear();
            ArrayList<U> urls = new ArrayList(this.urls);
            for (Url u : urls) {
                u.setCluster(null);
            }
            stream.remove(this);
            if (watch) {
                log.info("reBase remove cluster %s", base);
            }
            return false;
        } else {
            if (!base.equals(this.base)) {
                stream.changedclusters.add(id);
                setBase(base);
            }
            return true;
        }
    }

    public int getID() {
        return id;
    }

    public void addUrl(U url) {
        if (url.watch)
            log.info("Add Url %d to cluster %d", url.getID(), getID());
        if (urls.contains(url)) {
            log.info("Cluster %d", getID());
            for (Url u : urls) {
                log.info("contains %d", u.getID());
            }
            log.fatal("duplicate cluster %d url %d", id, url.getID());
        }
        stream.changedclusters.add(id);
        urls.add(url);
        if (watch) {
            log.info("addUrl cluster %d url %d", id, url.getID());
        }
    }

    public void addUrlDontCheck(U url) {
        if (!urls.contains(url)) {
            if (watch) {
                log.info("addUrlDontCheck cluster %d url %d", id, url.getID());
            }
            stream.changedclusters.add(id);
            urls.add(url);
        }
    }

    public ArrayList<U> getUrls() {
        return urls;
    }

    public HashSet<U> getBase() {
        return base;
    }

    public HashSet<Url> linkedTo(HashSet<Url> set) {
        HashSet<Url> result = new HashSet();
        for (Url u : urls) {
            if (u.linkedTo(set)) {
                result.add(u);
            }
        }
        return result;
    }

    public HashSet<Url> linkedToNoBase(HashSet<Url> set) {
        HashSet<Url> result = new HashSet();
        for (Url u : urls) {
            if (!base.contains(u) && u.linkedTo(set)) {
                result.add(u);
            }
        }
        return result;
    }

    public void remove(Url url) {
        if (watch || url.watch) {
            log.info("remove cluster %d url %d", id, url.getID());
        }
        stream.changedclusters.add(id);
        getUrls().remove(url);
    }

    static class Url2<Url> extends HashMap<Url, HashSet<Url>> {

        void addEdge(Url url, Url edge) {
            HashSet<Url> list = get(url);
            if (list == null) {
                list = new HashSet();
                put(url, list);
            }
            list.add(edge);
        }
    }

    public static final HashSet<Url> emptylist = new HashSet();

    public static HashSet<Url> getBase(Collection<? extends Url> base) {
        HashSet<Url> result = new HashSet(base);
        Url2<Url> edges = new Url2();
        HashSet<Url> remove1 = new HashSet(base);
        for (Url u : result) {
            for (int e = 0; e < u.edges; e++) {
                Edge edge = u.getNN(e);
                Url to = edge.getUrl();
                if (to != null) {
                    if (to.getID() > u.getID() && result.contains(to) && to.linkedTo(u)) {
                        edges.addEdge(u, to);
                        edges.addEdge(to, u);
                        remove1.remove(to);
                        remove1.remove(u);
                    }
                }
            }
        }

        if (remove1.size() > 0) {
            if (result.size() - remove1.size() <= Cluster.BREAKTIE) {
                return emptylist;
            }
            result.removeAll(remove1);
        }

        ArrayList<Url> remove = new ArrayList();
        while (true) {
            for (Map.Entry<Url, HashSet<Url>> next : edges.entrySet()) {
                if (next.getValue().size() <= Cluster.BREAKTIE) {
                    if (result.size() <= Cluster.BREAKTIE + 1) {
                        return emptylist;
                    }
                    result.remove(next.getKey());
                    remove.add(next.getKey());
                }
            }
            if (remove.isEmpty()) {
                break;
            }
            for (Url u : remove) {
                for (Url v : edges.get(u)) {
                    HashSet<Url> links = edges.get(v);
                    links.remove(u);
                }
                edges.remove(u);
            }
            remove = new ArrayList();
        }
        if (result.size() < Cluster.K) {
            return emptylist;
        }
        return result;
    }

    public long getCreationTime() {
        long creationtime = Long.MAX_VALUE;
        for (Url url : urls) {
            if (url.getAvgScore() >= .5) {
                creationtime = Math.min(creationtime, url.getCreationTime());
            }
        }
        return creationtime;
    }

    public long getLastCreationTime() {
        long creationtime = Long.MIN_VALUE;
        for (Url url : urls) {
            if (url.getAvgScore() >= .5) {
                creationtime = Math.max(creationtime, url.getCreationTime());
            }
        }
        return creationtime;
    }

    public double getAvgBaseScore() {
        double basescore = 0;
        int count = 0;
        for (Url u : getBase()) {
            for (int i = 0; i < u.getEdges(); i++) {
                Edge e = u.getNN(i);
                if (getBase().contains(e.url)) {
                    basescore += e.getScore();
                    count++;
                }
            }
        }
        basescore /= count;
        return basescore;
    }

    public long getAvgBaseCreationTime() {
        long time = 0;
        for (Url u : getBase()) {
            time += u.getCreationTime();
        }
        time /= getBase().size();
        return time;
    }

    public int size() {
        return urls.size();
    }

    public int hashCode() {
        return id;
    }

    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        return Integer.toString(id);
//        StringBuilder sb = new StringBuilder();
//        sb.append(sprintf("\ncluster %d [%d] ", getID(), urls.size()));
//        for (Url url : urls) {
//            sb.append("\n").append(urls.toString());
//        }
//        return sb.toString();
    }

    public String evalall() {
        StringBuilder sb = new StringBuilder();
        sb.append(eval()).append("\n-----\n");
        HashSet<Url> covered = new HashSet(getBase());
        HashSet<Url> todo = new HashSet(getUrls());
        todo.removeAll(covered);
        int size = urls.size();
        while (size > todo.size()) {
            size = todo.size();
            Iterator<Url> iter = todo.iterator();
            while (iter.hasNext()) {
                Url u = iter.next();
                int count = 0;
                for (int i = 0; i < u.edges; i++) {
                    Url nn = u.getNN(i).getUrl();
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
            for (Url u : todo) {
                sb.append(u.getID()).append(": ");
                addEval(sb, u);
            }
        }
        return sb.toString();
    }

    public String eval() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("CLUSTER %d size=%d\n", id, urls.size()));
        for (Url u : base) {
            sb.append(u.getID()).append(":");
            addEval(sb, u);
        }
        return sb.toString();
    }

    private void addEval(StringBuilder sb, Url u) {
        LOOP:
        for (Edge edge : new EdgeIterator(u)) {
            Url l = edge.getUrl();
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

    public String represent() {
        HashMapDouble<String> countterms = new HashMapDouble();
        Tuple2<Url, Double> max = null;
        for (Url u : urls) {
            UrlS s = (UrlS) u;
            for (String term : s.getFeatures()) {
                countterms.add(term, 1);
            }
        }
        countterms.normalize();

        for (Url u : urls) {
            if (u.getAvgScore() >= 0.5) {
                UrlS s = (UrlS) u;
                double score = 0;
                for (String term : s.getFeatures()) {
                    score += countterms.get(term);
                }
                if (max == null || max.value2 < score) {
                    max = new Tuple2(u, score);
                }
            }
        }
        return max.value1.toString();
    }

    public static void dump(Collection<UrlS> urls) {
        for (UrlS url : urls) {
            log.printf("%s", url.toStringEdges());
        }
    }
}
