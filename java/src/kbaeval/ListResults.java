package kbaeval;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.io.Datafile;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
import static io.github.htools.lib.PrintTools.sprintf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author jeroen
 */
public class ListResults {

    public static final Log log = new Log(ListResults.class);
    HashMap<String, ArrayList<MatchWritable>> matched;
    HashMap<String, PoolWritable> pooled;
    HashMap<String, TrecWritable> results;

    public ListResults(String matchesfile, String poolfile, String trecfile) {
        pooled = getEPool(new Datafile(poolfile));
        matched = getEmatches(new Datafile(matchesfile));
        results = getresults(new Datafile(trecfile));
        for (Map.Entry<String, TrecWritable> entry : results.entrySet()) {
            ArrayList<MatchWritable> matches = matched.get(entry.getKey());
            if (matches != null) {
                PoolWritable p = pooled.get(entry.getKey());
                log.printf("\t%s\t%s\n", entry.getKey(), p.update_text);
                for (MatchWritable m : matches) {
                    if (m.match_start < 0) {
                        log.info("%d %s %s %d %d", m.query_id, m.update_id, m.nugget_id, m.match_start, m.match_end);
                    } else {
                        log.printf("%s\t%s\n", m.nugget_id, p.update_text.substring(Math.max(0, m.match_start), Math.min(p.update_text.length(), m.match_end + 1)));
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

    public HashMap<String, TrecWritable> getresults(Datafile in) {
        HashMap<String, TrecWritable> results = new HashMap();
        TrecFile pf = new TrecFile(in);
        for (TrecWritable w : pf) {
                results.put(w.document + "-" + w.sentence, w);
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
        ArgsParser conf = new ArgsParser(args, "-m matches -p pool -t trecfile");
        new ListResults(conf.get("matches"), conf.get("pool"), conf.get("trecfile"));
    }
}
