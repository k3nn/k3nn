package KNN;

import io.github.repir.tools.lib.Log;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 *
 * @author jeroen
 */
public class UrlD extends UrlT {

    static Log log = new Log(UrlD.class);
    public HashSet<String> features;
    public UUID uuid;
    public int sentence;

    public UrlD(int id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
        super(id, domain, title, features, creationtime);
        this.uuid = uuid;
        this.sentence = sentence;
        this.features = features;
    }
    
    public String getDocumentID() {
        return this.getCreationTime() + "-" + uuid.toString().replace("-", "");
    }
    
    @Override
    public HashSet<String> getFeatures() {
        return features;
    }
}
