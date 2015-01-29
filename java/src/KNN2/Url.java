package KNN2;

import KNN.Cluster;
import KNN.Edge;
import io.github.repir.tools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public abstract class Url extends KNN.Url {

    public static Log log = new Log(Url.class);
    HashSet<Url> backlinks;

    public Url(int id) {
        super(id);
    }

    public Url(Cluster c) {
        super(c);
    }

    public void setNN(int i, Url url) {
        if (edge[i] != null && edge[i].getUrl() != null) {
            ((Url)edge[i].getUrl()).removeBackLink(this);
        }
        edge[i] = new Edge(url, edge[i].getScore());
        if (url != null) {
            url.addBackLink(this);
        }
    }

    public ArrayList<Url> getNextBidirectionalEdges() {
        ArrayList<Url> result = new ArrayList();
        for (int e = 0; e < edges; e++) {
            Url dest = ((Url)edge[e].getUrl());
            if (dest != null && dest.linkedTo(this)) {
                result.add(dest);
            }
        }
        return result;
    }

    public void addBackLink(Url u) {
        if (backlinks == null) {
            backlinks = new HashSet();
        }
        backlinks.add(u);
    }

    public void removeBackLink(Url u) {
        backlinks.remove(u);
    }    
    
    public void add(Edge e) {
        if (edges == 0) {
            edge[edges++] = e;
            if (e.getUrl() != null) {
                ((Url)e.getUrl()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d 0 %d %f", getID(), e.getUrl() == null ? -1 : e.getUrl().getID(), e.getScore());
            }
        } else if (edge[edges - 1].getScore() < e.getScore()) {
            if (edges == Cluster.K && edge[Cluster.K - 1].getUrl() != null) {
                ((Url)edge[Cluster.K - 1].getUrl()).removeBackLink(this);
            }
            for (int pos = 0; pos < edges; pos++) {
                if (edge[pos].getScore() < e.getScore()) {
                    for (int i = Math.min(Cluster.K - 1, edges); i > pos; i--) {
                        edge[i] = edge[i - 1];
                    }
                    edge[pos] = e;
                    if (e.getUrl() != null) {
                        ((Url)e.getUrl()).addBackLink(this);
                    }
                    if (watch) {
                        log.info("add Edge %d %d %d %f", getID(), pos, e.getUrl() == null ? -1 : e.getUrl().getID(), e.getScore());
                    }
                    if (edges < Cluster.K) {
                        edges++;
                    }
                    return;
                }
            }
        } else if (edges < Cluster.K) {
            edge[edges++] = e;
            if (e.getUrl() != null) {
                ((Url)e.getUrl()).addBackLink(this);
            }
            if (watch) {
                log.info("add Edge %d %d %d %f", getID(), edges - 1, e.getUrl() == null ? -1 : e.getUrl().getID(), e.getScore());
            }
        }
    }
}
