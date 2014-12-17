package KNN;

import io.github.repir.tools.Collection.ArrayMap;
import io.github.repir.tools.Collection.HashMapInt;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.Profiler;
import io.github.repir.tools.Type.Tuple2;
import io.github.repir.tools.Type.Tuple2Comparable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author jeroen
 */
public class IIUrls extends HashMap<String, HashSet<Url>> {

    public static final Log log = new Log(IIUrls.class);

    enum P {

        candidateUrlClusters
    }

    public void add(String term, Url url) {
        HashSet<Url> list = get(term);
        if (list == null) {
            list = new HashSet();
            put(term, list);
        }
        list.add(url);
    }

    public void add(Url url, Collection<String> features) {
        //log.info("add %d", url.getID());
        for (String term : features) {
            add(term, url);
        }
    }

    public HashSet<Url> candidateUrls(UrlS url) {
        Profiler.startTime(P.candidateUrlClusters.name());
        HashSet<Url> result = new HashSet();
        for (String term : url.getFeatures()) {
            HashSet<Url> clusters1 = get(term);
            if (clusters1 != null) {
                result.addAll(clusters1);
            }
        }
        Profiler.addTime(P.candidateUrlClusters.name());
        return result;
    }

    public ArrayMap<Tuple2Comparable<Integer, Long>, Url> candidateUrlsCount(Collection<String> features) {
        Profiler.startTime(P.candidateUrlClusters.name());
        HashMapInt<Url> map = new HashMapInt();
        for (String term : features) {
            HashSet<Url> clusters1 = get(term);
            if (clusters1 != null) {
                for (Url u : clusters1)
                    map.add(u, 1);
            }
        }
        ArrayMap<Tuple2Comparable<Integer, Long>, Url> result = new ArrayMap();
        for (Map.Entry<Url, Integer> entry : map.entrySet()) {
            Url url = entry.getKey();
            Tuple2Comparable<Integer, Long> key = new Tuple2Comparable(entry.getValue(), url.getCreationTime());
            result.add(key, entry.getKey());
        }
        Profiler.addTime(P.candidateUrlClusters.name());
        return result;
    }

    public void print() {
        log.printf("IIUrls size %d", size());
        int count = 0;
        for (Map.Entry<String, HashSet<Url>> entry : entrySet()) {
            if (count++ < 10) {
                log.printf("%s -> %d", entry.getKey(), entry.getValue().size());
            } else {
                break;
            }
        }
    }
}
