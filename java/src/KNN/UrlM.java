package KNN;

import io.github.repir.tools.lib.Log;
import java.util.Collection;

/**
 *
 * @author jeroen
 */
public class UrlM extends Url {

    static Log log = new Log(UrlM.class);
    //static long start2014 = DateTools.toDate(2014, 0, 1).getTime()/1000;
    protected int featureCount;
    
    protected UrlM(Cluster cluster) {
        super(cluster);
    }
    
    public UrlM(int id, int domain, long creationtime, Collection<String> features) {
        this(id, domain, creationtime, features.size());
    }

    public UrlM(int id, int domain, long creationtime, int features) {
        super(id);
        setCreationTime(creationtime);
        this.featureCount = features;
        this.setDomain(domain);
    }
    
    public int countFeatures() {
        return featureCount;
    }  
}
