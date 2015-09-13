package io.github.k3nn;

import io.github.htools.lib.Log;
import java.util.Iterator;
/**
 *
 * @author jeroen
 */
public class EdgeIterator<N extends Node> implements Iterator<Edge<N>>, Iterable<Edge<N>> {
   public static final Log log = new Log( EdgeIterator.class );
   private Node node;
   private int pos = 0;
   
   public EdgeIterator(N node) {
       this.node = node;
   }

    @Override
    public boolean hasNext() {
        return pos < node.countNearestNeighbors();
    }

    @Override
    public Edge<N> next() {
        if (hasNext())
            return node.getNearestNeighbor(pos++);
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Edge<N>> iterator() {
        return this;
    }
}
