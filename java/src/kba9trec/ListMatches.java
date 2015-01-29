package kba9trec;

import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import StreamCluster.UrlWritable;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
        pooled = getEPool(new Datafile(poolfile));
        matched = getEmatches(new Datafile(matchesfile));
        TreeMap<String, PoolWritable> sortedpool = new TreeMap(pooled);
        for (PoolWritable p : sortedpool.values()) {
            ArrayList<MatchWritable> get = matched.get(p.update_id);
            if (get == null) {
                log.printf("\t%s\n\n", p.update_text);
            } else {
                log.printf("\t%s\n", p.update_text);
                for (MatchWritable m : get) {
                    if (m.match_start < 0) {
                        log.info("%d %s %s %d %d", m.query_id, m.update_id, m.nugget_id, m.match_start, m.match_end);
                    } else {
                        log.printf("%s\t%s\n", m.nugget_id, p.update_text.substring(Math.max(0, m.match_start - 1), Math.min(p.update_text.length(), m.match_end)));
                    }
                }
                log.printf("\n");
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
        StreamClusterFile tf = new StreamClusterFile(df);
        for (StreamClusterWritable t : tf) {
            for (UrlWritable u : t.urls) {
                String updateid = sprintf("%s-%d", u.docid, u.row);
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
