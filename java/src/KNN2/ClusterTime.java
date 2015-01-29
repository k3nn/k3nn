package KNN2;

import KNN.Cluster;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.DateTools;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceWritable;

/**
 *
 * @author jeroen
 */
public class ClusterTime {

    public static final Log log = new Log(ClusterTime.class);
    DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();
    TreeMap<Integer, ArrayList<UrlF>> workload;
    Stream<UrlM> stream = new Stream();
    ArrayList<Integer> times = new ArrayList();

    public static void main(String args[]) throws ParseException {
        new ClusterTime();
    }

    public ClusterTime() throws ParseException {
        readWorkLoad();
        long starttime = System.currentTimeMillis();
        long nexttime = System.currentTimeMillis() + 100;
        for (Map.Entry<Integer, ArrayList<UrlF>> entry : workload.entrySet()) {
            ArrayList<UrlF> list = entry.getValue();
            for (UrlF url : list) {
                stream.urls.put(url.getID(), url);
                stream.add(url, url.getFeatures());
                while (System.currentTimeMillis() >= nexttime) {
                    times.add(stream.urls.size());
                    log.printf("%.1f %d", (nexttime - starttime)/1000.0, stream.urls.size());
                    nexttime += 100;
                }
            }
            if (entry.getKey() > 3) {
                Date startdate = DateTools.FORMAT.Y_M_D.parse("2011-11-06");
                startdate = DateTools.daysAfter(startdate, entry.getKey() - 3);
                long purgethresh = startdate.getTime() / 1000;
                HashSet<Integer> keepclusters = new HashSet();
                for (Cluster<UrlM> c : stream.clusters.values()) {
                    for (UrlM u : c.getUrls()) {
                        if (u.getCreationTime() >= purgethresh) {
                            keepclusters.add(c.getID());
                            break;
                        }
                    }
                }
                int oldsize = stream.urls.size();
                for (Map.Entry<String, HashSet<UrlM>> ii : stream.iiurls.entrySet()) {
                    Iterator<UrlM> iter = ii.getValue().iterator();
                    while (iter.hasNext()) {
                        UrlM u = iter.next();
                        if (u.getCreationTime() < purgethresh) {
                            if (!u.isClustered() || !keepclusters.contains(u.getCluster().getID())) {
                                iter.remove();
                                stream.urls.remove(u.getID());
                            }
                        }
                    }
                    while (System.currentTimeMillis() >= nexttime) {
                        times.add(oldsize);
                        log.printf("%.1f %d", (nexttime - starttime)/1000.0, oldsize);
                        nexttime += 100;
                    }
                }
            }
        }
        log.info("%s", times);
    }

    public void readWorkLoad() {
        workload = new TreeMap();
        for (int day = 6; day < 26; day++) {
            String filename = sprintf("Downloads/2011-11-%02d", day);
            Datafile df = new Datafile(filename);
            SentenceFile sf = new SentenceFile(df);
            ArrayList<UrlF> list = new ArrayList();
            for (SentenceWritable value : sf) {
                ArrayList<String> features = tokenizer.tokenize(value.sentence);
                if (features.size() > 1) {
                    HashSet<String> uniq = new HashSet(features);
                    UrlF url = new UrlF(value.id, value.domain, uniq, value.creationtime);
                    list.add(url);
                }
            }
            workload.put(workload.size(), list);
        }
    }
}
