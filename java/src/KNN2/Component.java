package KNN2;

import KNN.*;
import io.github.repir.tools.lib.Log;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public abstract class Component {

    public static Log log = new Log(Component.class);
    static int idd = 0;
    int id = idd++;
    HashSet<String> features = new HashSet();
    long creationtime = 0;
    int frequency = 0;

    abstract HashSet<String> getFeatures();

    abstract long getCreationTime();

    abstract int size();
    
    public int hashCode() {
        return id;
    }
    
    public boolean equals(Object o) {
        return this == o;
    }
}
