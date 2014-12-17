package KNN;

import io.github.repir.tools.Collection.HashMapDouble;
import io.github.repir.tools.Lib.Log;
import static io.github.repir.tools.Lib.PrintTools.sprintf;
import io.github.repir.tools.Lib.Profiler;
import io.github.repir.tools.Type.Tuple2;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class Cluster {

    public static final Log log = new Log(Cluster.class);
    public static final int K = 3;
    public static final int BREAKTIE = K / 2;
    public static HashMap<Integer, Cluster> clusters = new HashMap();
    public static int watch = -1;
    static int idd = 0;
    int id = idd++;
    private ArrayList<Url> urls = new ArrayList();
    private ArrayList<Url> base = new ArrayList();
    public final Url dummy;

    public Cluster(Collection<Url> base) {
        setBase(base);
        for (Url u : base) {
            u.setCluster(this);
        }

        clusters.put(id, this);
        dummy = new UrlM(this);
    }

    public Cluster(int id) {
        this.id = id;
        idd = Math.max(id + 1, idd);
        clusters.put(id, this);
        if (id == watch) {
            log.info("create %d", id);
        }
        dummy = new UrlM(this);
    }
    
    public static Cluster get(int id) {
        return clusters.get(id);
    }

    public static Collection<Cluster> getClusters() {
        return clusters.values();
    }

    public void setBase(Collection<Url> base) {
        if (base.size() == 2) {
            //log.crash();
        }
        this.base = new ArrayList(base);
    }

    public boolean recheckBase() {
        HashSet<Url> base = getBase(urls);
        if (id == watch) {
            log.info("reBase %s", base);
        }
        if (base.isEmpty()) {
            this.base.clear();
            for (Url u : urls) {
                u.setCluster(null);
            }
            Cluster.clusters.remove(id);
            if (id == watch) {
               log.info("reBase remove cluster %s", base);
            }
            return false;
        } else {
            if (!base.equals(this.base)) {
                setBase(base);
            }
            return true;
        }
    }

    public int getID() {
        return id;
    }

    public void addUrl(Url url) {
        if (urls.contains(url)) {
            log.info("Cluster %d", getID());
            for (Url u : urls) {
                log.info("contains %d", u.getID());
            }
            log.fatal("duplicate cluster %d url %d", id, url.getID());
        }
        urls.add(url);
        if (id == watch) {
            //log.info("addUrl %d freq %d", url.id, url.getFrequency());
        }
    }
    
    public void addUrlDontCheck(Url url) {
        if (!urls.contains(url)) {
           urls.add(url);
        }
    }
    
    public ArrayList<Url> getUrls() {
        return urls;
    }

    public ArrayList<Url> getBase() {
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
            getUrls().remove(url);
    }
    
    public void remove(final Url url, HashSet<Url> changed) {
        Profiler.startTime("remove");
        if (id == watch) {
            log.info("remove %d url %d", id, url.getID());
        }
        HashSet<Url> linked = new HashSet();
        HashSet<Url> newlinked = new HashSet() {
            {
                add(url);
            }
        };
        while (!newlinked.isEmpty()) {
            urls.removeAll(newlinked);
            linked.addAll(newlinked);
            newlinked = linkedToNoBase(newlinked);
        }
        for (Url entry : linked) {
            if (id == watch) {
                log.info("remove linked url %d", url.getID());
            }
            if (!changed.contains(entry)) {
//                if (entry == Stream.watch) {
//                    log.info("removed %d %d %s", id, size(), entry.url);
//                }
                changed.add(entry);
                entry.setCluster(null);
            }
        }
        if (id == watch) {
            log.info(urls);
            log.crash();
        }
        Profiler.addTime("remove");
    }

    public static void addMajority(HashSet<Url> urls) {
        Iterator<Url> iter = urls.iterator();
        while (iter.hasNext()) {
            Url u = iter.next();
            Cluster c = u.majority();
            if (c != null) {
                u.setCluster(c);
                iter.remove();
            }
        }
    }
    
    
    static class Url2 extends HashMap<Url, HashSet<Url>> {

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
    
    public static HashSet<Url> getBase(Collection<Url> base) {
        Profiler.startTime("getBase");
        HashSet<Url> result = new HashSet(base);
        Url2 edges = new Url2();
        HashSet<Url> remove1 = new HashSet(base);
        for (Url u : result) {
            for (int e = 0; e < u.edges; e++) {
                Edge edge = u.getNN(e);
                Url to = edge.getUrl();
                if (to.getID() > u.getID() && result.contains(to) && to.linkedTo(u)) {
                    edges.addEdge(u, to);
                    edges.addEdge(to, u);
                    remove1.remove(to);
                    remove1.remove(u);
                }
            }
        }

        if (remove1.size() > 0) {
            if (result.size() - remove1.size() <= Cluster.BREAKTIE) {
                Profiler.addTime("getBase");
                return emptylist;
            }
            result.removeAll(remove1);
        }

        ArrayList<Url> remove = new ArrayList();
        while (true) {
            for (Map.Entry<Url, HashSet<Url>> next : edges.entrySet()) {
                if (next.getValue().size() <= Cluster.BREAKTIE) {
                    if (result.size() <= Cluster.BREAKTIE + 1) {
                        Profiler.addTime("getBase");
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
        Profiler.addTime("getBase");
        if (result.size() < Cluster.K)
            return emptylist;
        return result;
    }

    public long getCreationTime() {
        long creationtime = Long.MAX_VALUE;
        for (Url url : urls) {
            if (url.getAvgScore() >= .5)
               creationtime = Math.min(creationtime, url.getCreationTime());
        }
        return creationtime;
    }

    public long getLastCreationTime() {
        long creationtime = Long.MIN_VALUE;
        for (Url url : urls) {
            if (url.getAvgScore() >= .5)
               creationtime = Math.max(creationtime, url.getCreationTime());
        }
        return creationtime;
    }

    public double getAvgBaseScore() {
        double basescore = 0;
        for (Url u : getBase()) {
            basescore += u.getNN(Cluster.BREAKTIE).getScore();
        }
        basescore /= getBase().size();
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
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("\ncluster [%d] ", urls.size()));
        for (int i = 0; i < urls.size(); i++) {
            sb.append("\n").append(urls.get(i).toString());
        }
        return sb.toString();
    }

    public String evalall() {
        StringBuilder sb = new StringBuilder();
        sb.append(eval()).append("\n-----");
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
                    if (covered.contains(u.getNN(i).url))
                        count++;
                }
                if (count > Cluster.BREAKTIE) {
                    iter.remove();
                    covered.add(u);
                    addEval(sb, u);
                }
            }
        }
        if (todo.size() > 0) {
            sb.append("\n---- misplaced ---");
            for (Url u : todo) {
                addEval(sb, u);
            }
        }
        return sb.toString();
    }
    
    public String eval() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("CLUSTER %d size=%d\n", id, urls.size()));
        for (int v = 0; v < base.size(); v++) {
            Url u = base.get(v);
            sb.append(urls.indexOf(u)).append(":");
            addEval(sb, u);
        }
        return sb.toString();
    }

    private void addEval(StringBuilder sb, Url u) {
        LOOP:
        for (Edge edge : new EdgeIterator(u)) {
            Url l = edge.getUrl();
            for (int j = 0; j < urls.size(); j++) {
                if (urls.get(j) == l) {
                    sb.append(j).append(" ");
                    continue LOOP;
                }
            }
            sb.append("x ");
        }
        sb.append(" ").append(u.toString()).append(" ").append(u.getAvgScore()).append("\n");
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
