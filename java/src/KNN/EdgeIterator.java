package KNN;

import io.github.repir.tools.lib.Log;
import java.util.Iterator;
/**
 *
 * @author jeroen
 */
public class EdgeIterator implements Iterator<Edge>, Iterable<Edge> {
   public static final Log log = new Log( EdgeIterator.class );
   private Url url;
   private int pos = 0;
   
   public EdgeIterator(Url url) {
       this.url = url;
   }

    @Override
    public boolean hasNext() {
        return pos < url.getEdges();
    }

    @Override
    public Edge next() {
        if (hasNext())
            return url.getNN(pos++);
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Edge> iterator() {
        return this;
    }
}
