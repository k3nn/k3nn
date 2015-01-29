package KNN2;

import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class UrlF extends UrlM {

    static Log log = new Log(UrlF.class);
    HashSet<String> features = new HashSet();

    public UrlF(int id, int domain, Collection<String> features, long creationtime) {
        super(id, domain, creationtime, features);
        this.setFeatures(features);
    }

    public HashSet<String> getFeatures() {
        return features;
    }
    
    public void setFeatures(Collection<String> features) {
        this.features.addAll(features);
        this.featureCount = this.features.size();
    }

    @Override
    public String toString() {
        if (1==1) {
            return sprintf("Url %d", getID());
        }
        StringBuilder sb = new StringBuilder();
        //log.info("toString %d %d %s", getID(), edges, features);
        sb.append(sprintf("Url [%d] %s", edges, features));
        //for (int i = 0; i < edges; i++) {
        //    sb.append(nn[i].toString());
        //}
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, features));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}
