package KNN;

import io.github.repir.tools.Lib.Log;
import static io.github.repir.tools.Lib.PrintTools.sprintf;
import java.util.Collection;

/**
 *
 * @author jeroen
 */
public class UrlT extends UrlM {

    static Log log = new Log(UrlT.class);
    String title;

    public UrlT(int id, int domain, String title, Collection<String> features, long creationtime) {
        super(id, domain, creationtime, features);
        this.title = title;
    }

    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, title));
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, title));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}
