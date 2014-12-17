package KNN;

import static KNN.Cluster.log;
import io.github.repir.tools.Collection.ArrayMap;
import io.github.repir.tools.Lib.Log;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

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
