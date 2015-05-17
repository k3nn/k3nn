package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import kbaeval.MatchesFile;

/**
 *
 * @author jeroen
 */
public class ListMatches {

    public static final Log log = new Log(ListMatches.class);
    HashMap<String, ArrayList<MatchWritable>> matched;
    HashMap<String, PoolWritable> pooled;

    public ListMatches(String matchesfile, String poolfile) {
        HashMap<String, ArrayList<String>> matches = new HashMap();
        pooled = getEPool(new Datafile(poolfile));
        matched = getEmatches(new Datafile(matchesfile));
        TreeMap<String, PoolWritable> sortedpool = new TreeMap(pooled);
        for (PoolWritable p : sortedpool.values()) {
            ArrayList<MatchWritable> get = matched.get(p.update_id);
            if (get != null) {
                for (MatchWritable m : get) {
                    if (m.match_start >= 0) {
                        ArrayList<String> list = matches.get(m.nugget_id);
                        if (list == null) {
                            list = new ArrayList();
                            matches.put(m.nugget_id, list);
                        }
                        String text = p.update_text.substring(Math.max(0, m.match_start), Math.min(p.update_text.length(), m.match_end + 1));
                        list.add(p.update_id + "\t" + text);
                    }
                }
            }
        }
        TreeMap<String, ArrayList<String>> sorted = new TreeMap(matches);
        for (Map.Entry<String, ArrayList<String>> entry : sorted.entrySet()) {
            Collections.sort(entry.getValue());
            for (String t : entry.getValue()) {
                log.printf("%s %s", entry.getKey(), t);
            }
        } 
    }

    public Set<String> readMatches(String matchesfile) {
        Datafile df = new Datafile(matchesfile);
        MatchesFile mf = new MatchesFile(df);
        return mf.getMap().keySet();
    }

    public HashMap<String, PoolWritable> getEPool(Datafile in) {
        HashMap<String, PoolWritable> results = new HashMap();
        PoolFile pf = new PoolFile(in);
        for (PoolWritable w : pf) {
            if (w.query_id < 11) {
                PoolWritable existing = results.get(w.update_id);
                if (existing != null) {
                    log.info("duplicate %s %s %s %s", existing.query_id, existing.update_id, w.query_id, w.update_id);
                }
                results.put(w.update_id, w);
            }
        }
        return results;
    }

    public HashMap<String, ArrayList<MatchWritable>> getEmatches(Datafile in) {
        HashMap<String, ArrayList<MatchWritable>> results = new HashMap();
        MatchFile pf = new MatchFile(in);
        for (MatchWritable mw : pf) {
            ArrayList<MatchWritable> list = results.get(mw.update_id);
            if (list == null) {
                list = new ArrayList();
                results.put(mw.update_id, list);
            }
            list.add(mw);
        }
        return results;
    }

    public HashSet<String> readResults(String resultsfile) {
        HashSet<String> result = new HashSet();
        Datafile df = new Datafile(HDFSPath.getFS(), resultsfile);
        df.setBufferSize(100000000);
        ClusterFile tf = new ClusterFile(df);
        for (ClusterWritable t : tf) {
            for (NodeWritable u : t.nodes) {
                String updateid = sprintf("%s-%d", u.docid, u.sentenceNumber);
                result.add(updateid);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        ArgsParser conf = new ArgsParser(args, "-m matches -p pool");
        new ListMatches(conf.get("matches"), conf.get("pool"));
    }
}
