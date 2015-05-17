package kbaeval;

import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.StrTools;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class ConvertMatchEditFile {

    private static final Log log = new Log(ConvertMatchEditFile.class);
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();
    MatchingClusterNodeFile clusterfile;

    public ConvertMatchEditFile(Datafile pool, Datafile matchedit, Datafile match) {
        HashMap<String, PoolWritable> pooled = getEPool(pool);
        ArrayList<MatchWritable> matched = getEmatches(matchedit, pooled);
        //log.info(ematches);

        Collections.sort(matched, new Sorter());
        MatchFile mf = new MatchFile(match);
        mf.openWrite();
        for (MatchWritable m : matched) {
            m.write(mf);
        }
        mf.closeWrite();
    }

    class Sorter implements Comparator<MatchWritable> {

        @Override
        public int compare(MatchWritable o1, MatchWritable o2) {
            int comp = o1.query_id - o2.query_id;
            if (comp == 0) {
                comp = o1.update_id.compareTo(o2.update_id);
                if (comp == 0) {
                    comp = o1.nugget_id.compareTo(o2.nugget_id);
                }
            }
            return comp;
        }

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

    public ArrayList<MatchWritable> getEmatches(Datafile in, HashMap<String, PoolWritable> updates) {
        ArrayList<MatchWritable> results = new ArrayList();
        MatchEditFile pf = new MatchEditFile(in);
        for (MatchEditWritable w : pf) {
            if (w.nugget_id != null && w.nugget_id.length() > 0) {
                if (w.update_id != null) {
                    PoolWritable update = updates.get(w.update_id);
                    String text = update.update_text;
                    MatchWritable mw = new MatchWritable();
                    mw.update_id = w.update_id;
                    mw.nugget_id = w.nugget_id;
                    mw.query_id = w.query_id;
                    mw.match_start = text.indexOf(w.match);
                    mw.match_end = mw.match_start + w.match.length() - 1;
                    results.add(mw);
                    log.info("%s %s %s", mw.update_id, mw.query_id, mw.nugget_id);
                }
            }
        }
        return results;
    }

    public static void main(String args[]) {
        ArgsParser ap = new ArgsParser(args, "pool matchedit match");
        Datafile pool = new Datafile(ap.get("pool"));
        Datafile matchedit = new Datafile(ap.get("matchedit"));
        Datafile match = new Datafile(ap.get("match"));
        new ConvertMatchEditFile(pool, matchedit, match);
    }
}
