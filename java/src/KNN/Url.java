package KNN;

import io.github.repir.tools.Lib.Log;
import static io.github.repir.tools.Lib.PrintTools.sprintf;
import io.github.repir.tools.Lib.Profiler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public abstract class Url implements Comparable<Url>{
    public static Log log = new Log(Url.class);
    private int id;
    private int domain = -1;
    private long creationtime;
    Edge[] edge = new Edge[Cluster.K];
    protected int edges = 0;
    private Cluster cluster = null;
    
    public Url(int id) {
        this.id = id;
    }
    
    public Url(Cluster c) {
        this.id = -2;
        cluster = c;
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
        Profiler.startTime("getFreeBiDirectional");
        HashSet<Url> list = new HashSet();
        HashSet<Url> newlist = new HashSet();
        newlist.add(this);
        do {
            HashSet<Url> nextlist = new HashSet();
            for (Url u : newlist) {
                list.add(u);
                for (Edge edge : new EdgeIterator(u)) {
                    Url c = edge.getUrl();
                    if (!c.isClustered() && !list.contains(c) && c.linkedTo(u)) {
                        nextlist.add(c);
                    }
                }
            }
            newlist = nextlist;
        } while (!newlist.isEmpty());
        Profiler.addTime("getFreeBiDirectional");
        return list;
    }
   
    public void clusterMajorityBase() {
        Profiler.startTime("clusterMajorityBase");
        HashSet<Url> list = getFreeBiDirectional();
        if (list.size() > Cluster.BREAKTIE) {
            HashSet<Url> base = Cluster.getBase(list);
            if (!base.isEmpty()) {
                Cluster newcluster = new Cluster(base);
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
        Profiler.addTime("clusterMajorityBase");
    }

    public Cluster majority() {
        Profiler.startTime("majority");
        Cluster cluster = null;
        if (getEdges() > Cluster.BREAKTIE + 1) {
            LOOP:
            for (int i = Cluster.K - Cluster.BREAKTIE - 1; i >= 0; i--) {
                Edge edge = getNN(i);
                if (edge == null)
                    log.info("majority url %d edges %d edge %d NULL", id, getEdges(), i);
                Url nn = edge.getUrl();
                if (nn == null)
                    log.info("majority url %d edges %d edge %d nn NULL", id, getEdges(), i);
                cluster = nn.getCluster();
                if (cluster != null) {
                   int equal = Cluster.BREAKTIE - i;
                   for (int j = i + 1; j < Cluster.K; j++) {
                       if (cluster != getNN(j).getUrl().getCluster()) {
                           if (--equal < 0) {
                               cluster = null;
                               continue LOOP;
                           }
                       }     
                   }
                   break LOOP;
                }
            }
        } else if (getEdges() > Cluster.BREAKTIE) {
            cluster = getNN(0).getUrl().getCluster();
            if (cluster != null) {
                for (int i = 1; i <= Cluster.BREAKTIE; i++) {
                    if (cluster != getNN(i).getUrl().getCluster()) {
                        cluster = null;
                        break;
                    }
                }
            }
        } 
        Profiler.addTime("majority");
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

    public boolean linkedTo(HashSet<Url> urls) {
        for (int i = 0; i < getEdges(); i++) {
            if (urls.contains(getNN(i).getUrl())) {
                return true;
            }
        }
        return false;
    }
    public int getEdges() {
        return edges;
    }

    public Double getLowestScore() {
        return edge[Cluster.K - 1] == null ? 0 : edge[Cluster.K - 1].score;
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
        this.cluster = c;
        if (c != null) {
            //log.info("setCluster url %d cluster %d", getID(), c.getID());
            c.addUrl(this);
        }
    }
    
    public void setClusterDontCheck(Cluster c) {
        this.cluster = c;
        if (c != null)
            c.addUrlDontCheck(this);
    }
    
    public Cluster getCluster() {
        return cluster;
    }
    
    public void add(Edge e) {
        if (edges == 0) {
            edge[edges++] = e;
        } else if (edge[edges - 1].score < e.score) {
            for (int pos = 0; pos < edges; pos++) {
                if (edge[pos].score < e.score) {
                    for (int i = Math.min(Cluster.K - 1, edges); i > pos; i--) {
                        edge[i] = edge[i - 1];
                    }
                    edge[pos] = e;
                    if (edges < Cluster.K) {
                        edges++;
                    }
                    return;
                }
            }
        } else if (edges < Cluster.K) {
            edge[edges++] = e;
        }
    }

    public String getNN() {
        StringBuilder sb = new StringBuilder();
            for (int i = 0; i < getEdges(); i++)
                sb.append(",").append(getNN(i).getUrl().getID());
        return (getEdges() > 0)?sb.deleteCharAt(0).toString():"";
    }
    
    public String getScore() {
        StringBuilder sb = new StringBuilder();
            for (int i = 0; i < getEdges(); i++)
                sb.append(",").append(getNN(i).getScore());
        return (getEdges() > 0)?sb.deleteCharAt(0).toString():"";
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

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %d", edges, id));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}
