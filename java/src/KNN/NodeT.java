package KNN;

import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.Collection;

/**
 * Extension of Node that also holds a String of its contents.
 * @author jeroen
 */
public class NodeT extends NodeM {

    static Log log = new Log(NodeT.class);
    String content; // content the node represents

    public NodeT(long id, int domain, String title, Collection<String> features, long creationtime) {
        super(id, domain, creationtime, features);
        this.content = title;
    }

    public String getContent() {
        return content;
    }
    
    public void setContent(String title) {
        this.content = title;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url %d [%d] %s", getID(), edges, content));
        return sb.toString();
    }

    public String toStringEdges() {
        StringBuilder sb = new StringBuilder();
        sb.append(sprintf("Url [%d] %s", edges, content));
        for (int i = 0; i < edges; i++) {
            sb.append("\n").append(edge[i].toString());
        }
        return sb.toString();
    }
}
