package KNN;

import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.Profiler;
import io.github.repir.tools.type.Tuple2Comparable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class IIUrls<U extends Url> extends HashMap<String, HashSet<U>> {

    public static final Log log = new Log(IIUrls.class);

    enum P {

        candidateUrlClusters
    }

    public void add(String term, U url) {
        HashSet<U> list = get(term);
        if (list == null) {
            list = new HashSet();
            put(term, list);
        }
        list.add(url);
    }

    public void add(U url, Collection<String> features) {
        //log.info("add %d", url.getID());
        for (String term : features) {
            add(term, url);
        }
    }

    public HashSet<Url> candidateUrls(UrlS url) {
        HashSet<Url> result = new HashSet();
        for (String term : url.getFeatures()) {
            HashSet<U> clusters1 = get(term);
            if (clusters1 != null) {
                result.addAll(clusters1);
            }
        }
        return result;
    }

    public ArrayMap<Tuple2Comparable<Integer, Integer>, U> candidateUrlsCount(Collection<String> features) {
        HashMapInt<U> map = new HashMapInt();
        for (String term : features) {
            HashSet<U> clusters1 = get(term);
            if (clusters1 != null) {
                for (U u : clusters1)
                    map.add(u, 1);
            }
        }
        ArrayMap<Tuple2Comparable<Integer, Integer>, U> result = new ArrayMap();
        for (Map.Entry<U, Integer> entry : map.entrySet()) {
            Url url = entry.getKey();
            Tuple2Comparable<Integer, Integer> key = new Tuple2Comparable(entry.getValue(), url.getID());
            result.add(key, entry.getKey());
        }
        return result;
    }
    
    public void print() {
        log.printf("IIUrls size %d", size());
        int count = 0;
        for (Map.Entry<String, HashSet<U>> entry : entrySet()) {
            if (count++ < 10) {
                log.printf("%s -> %d", entry.getKey(), entry.getValue().size());
            } else {
                break;
            }
        }
    }
}
