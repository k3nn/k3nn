package KNN;

import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.lib.Profiler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public abstract class Url implements Comparable<Url> {

    public static Log log = new Log(Url.class);
    public static final HashSet<Integer> allclusters = new HashSet();
    public static HashSet<Integer> watchlist = new HashSet(Arrays.asList());
    public static HashSet<Integer> listenurls;
    public static HashSet<Integer> listenclusters;
    public static HashSet<Url> modifiedurls = new HashSet();
    private int id;
    public final boolean watch;
    protected int domain = -1;
    protected long creationtime;
    protected Edge[] edge = new Edge[Cluster.K];
    protected int edges = 0;
    protected Cluster cluster = null;

    public Url(int id) {
        this.id = id;
        watch = watch(this);
    }

    public Url(Cluster c) {
        this.id = -2;
        cluster = c;
        watch = false;
    }

    private static boolean watch(Url url) {
        return watchlist.contains(url.getID());
    }

    public int getID() {
        return id;
    }

    public Edge getNN(int i) {
        return edge[i];
    }

    public void setNN(int i, Url url) {
        edge[i] = new Edge(url, edge[i].getScore());
    }

    public void setCreationTime(long creationtime) {
        this.creationtime = creationtime;
    }

    public int getDomain() {
        return domain;
    }

    public void setDomain(int domain) {
        this.domain = domain;
    }

    public String getUrl() {
        throw new UnsupportedOperationException();
    }

    public String getTitle() {
        throw new UnsupportedOperationException();
    }

    public HashSet<String> getFeatures() {
        throw new UnsupportedOperationException();
    }

    public void setFeatures(Collection<String> terms) {
        throw new UnsupportedOperationException();
    }

    public void setTitle(String title) {
        throw new UnsupportedOperationException();
    }

    public void setUrl(String url) {
        throw new UnsupportedOperationException();
    }

    public HashSet<Url> getFreeBiDirectional() {
        HashSet<Url> list = new HashSet();
        HashSet<Url> newlist = new HashSet();
        newlist.add(this);
        do {
            HashSet<Url> nextlist = new HashSet();
            for (Url u : newlist) {
                list.add(u);
                for (Edge edge : new EdgeIterator(u)) {
                    Url c = edge.getUrl();
                    if (c != null && !c.isClustered() && !list.contains(c) && c.linkedTo(u)) {
                        nextlist.add(c);
                    }
                }
            }
            newlist = nextlist;
        } while (!newlist.isEmpty());
        return list;
    }

    public void clusterMajorityBase(Stream stream) {
        HashSet<Url> list = getFreeBiDirectional();
        if (list.size() > Cluster.BREAKTIE) {
            HashSet<Url> base = Cluster.getBase(list);
            if (!base.isEmpty()) {
                Cluster newcluster = stream.createCluster(base);
                //for (Url b : base) {
                //    log.info("base cluster %d url %d %d", newcluster.getID(), b.getID(), System.identityHashCode(b));
                // }
                boolean changed = true;
                list.removeAll(base);
                //for (Url b : list) {
                //    log.info("list cluster %d url %d %d", newcluster.getID(), b.getID(), System.identityHashCode(b));
                // }
                while (changed) {
                    changed = false;
                    ArrayList<Url> add = new ArrayList();
                    for (Url u : list) {
                        if (u.majority() == newcluster) {
                            add.add(u);
                        }
                    }
                    if (add.size() > 0) {
                        changed = true;
                        for (Url u : add) {
                            u.setCluster(newcluster);
                            list.remove(u);
                        }
                    }
                }
                //for (Url u : newcluster.urls)
                //    log.printf("%s", u.url);
            }
        }
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
                Url nn = edge.getUrl();
                if (nn == null) {
                    log.info("majority url %d edges %d edge %d nn NULL", id, getEdges(), i);
                } else {
                    cluster = nn.getCluster();
                    if (cluster != null) {
                        int equal = Cluster.BREAKTIE - i;
                        for (int j = i + 1; j < Cluster.K; j++) {
                            Url nnj = getNN(j).getUrl();
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
            Url nn0 = getNN(0).getUrl();
            if (nn0 != null) {
                cluster = nn0.getCluster();
                if (cluster != null) {
                    for (int i = 1; i <= Cluster.BREAKTIE; i++) {
                        Url nn = getNN(i).getUrl();
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

    public boolean linkedTo(Url urlto) {
        //Profiler.startTime("linkedTo");
        for (int i = 0; i < getEdges(); i++) {
            if (getNN(i).getUrl() == urlto) {
                //Profiler.addTime("linkedTo");
                return true;
            }
        }
        //Profiler.addTime("linkedTo");
        return false;
    }

    public <U extends Url> boolean linkedTo(HashSet<U> urls) {
        for (int i = 0; i < getEdges(); i++) {
            Url url = getNN(i).getUrl();
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

    public Url getLowestNN() {
        return edges == 0 ? null : edge[edges - 1].url;
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
        return id;
    }

    public boolean equals(Object o) {
        return this == o;
    }

    public boolean isClustered() {
        return cluster != null;
    }

    public void setCluster(Cluster c) {
        if (cluster != c) {
            if (this.cluster != null && listenclusters != null) {
                listenclusters.remove(this.cluster.getID());
            }
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
                if (listenurls != null) {
                    if (listenurls.contains(getID())) {
                        listenclusters.add(c.getID());
                        modifiedurls.add(this);
                    } else if (listenclusters.contains(c.getID())) {
                        modifiedurls.add(this);
                    }
                } else if (listenclusters == allclusters) {
                    modifiedurls.add(this);
                }
            } else if (watch) {
                log.info("unCluster %s", this.toClusterString());
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void add(Edge e) {
        if (edges == 0) {
            edge[edges++] = e;
            if (watch) {
                log.info("add Edge %d 0 %d %f", getID(), e.url == null ? -1 : e.url.getID(), e.score);
            }
        } else if (edge[edges - 1].score < e.score) {
            for (int pos = 0; pos < edges; pos++) {
                if (edge[pos].score < e.score) {
                    for (int i = Math.min(Cluster.K - 1, edges); i > pos; i--) {
                        edge[i] = edge[i - 1];
                    }
                    edge[pos] = e;
                    if (watch) {
                        log.info("add Edge %d %d %d %f", getID(), pos, e.url == null ? -1 : e.url.getID(), e.score);
                    }
                    if (edges < Cluster.K) {
                        edges++;
                    }
                    return;
                }
            }
        } else if (edges < Cluster.K) {
            edge[edges++] = e;
            if (watch) {
                log.info("add Edge %d %d %d %f", getID(), edges - 1, e.url == null ? -1 : e.url.getID(), e.score);
            }
        }
    }

    public String getNN() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getEdges(); i++) {
            Url url = getNN(i).getUrl();
            if (url != null) {
                sb.append(",").append(getNN(i).getUrl().getID());
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
    public int compareTo(Url o) {
        return getLowestScore() < o.getLowestScore() ? -1 : 1;
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
            if (e.getUrl() == null) {
                sb.append(sprintf("x [-1] "));
            } else {
                sb.append(sprintf("%d [%d] ", e.getUrl().id, e.getUrl().cluster == null ? -1 : e.getUrl().cluster.getID()));
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
