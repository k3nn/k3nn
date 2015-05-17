package kbapool;

import kbaeval.*;
import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.StrTools;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class CreatePoolFileMatch {

    private static final Log log = new Log(CreatePoolFileMatch.class);
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();

    public CreatePoolFileMatch(Datafile createpool, Datafile creatematch, Datafile newpool, Datafile existingpool, ArrayList<Datafile> inematch) {
        HashMap<String, PoolWritable> nPool = getPool(newpool);
        HashMap<String, PoolWritable> ePool = getPool(existingpool);

        ArrayList<PoolWritable> pool = new ArrayList(nPool.values());
        for (PoolWritable p : ePool.values()) {
            if (!nPool.containsKey(p.update_id))
                pool.add(p);
        }
        Collections.sort(pool, new SorterPool());
        PoolFile poolfile = new PoolFile(createpool);
        poolfile.openWrite();
        for (PoolWritable p : pool) {
            p.write(poolfile);
        }
        poolfile.closeWrite();

        ArrayList<MatchEditWritable> matched = new ArrayList();
        HashMap<String, HashMap<String, MatchEditWritable>> ematches = getEmatches(inematch, ePool);
        for (PoolWritable record : nPool.values()) {
            HashMap<String, MatchEditWritable> list = ematches.get(record.update_id);
            if (list != null) {
                for (MatchEditWritable match : list.values()) {
                    matched.add(match);
                }
            } else if (!ePool.containsKey(record.update_id)) {
                MatchEditWritable match = new MatchEditWritable();
                match.query_id = record.query_id;
                match.update_id = record.update_id;
                match.match = record.update_text;
                match.nugget_id = "NEW";
                matched.add(match);
            } else {
                MatchEditWritable match = new MatchEditWritable();
                match.query_id = record.query_id;
                match.update_id = record.update_id;
                match.match = record.update_text;
                match.nugget_id = "";
                matched.add(match);
            }
        }
        Collections.sort(matched, new Sorter());
        MatchEditFile mf = new MatchEditFile(creatematch);
        mf.openWrite();
        for (MatchEditWritable m : matched) {
            m.write(mf);
        }
        mf.closeWrite();
    }

    class Sorter implements Comparator<MatchEditWritable> {

        @Override
        public int compare(MatchEditWritable o1, MatchEditWritable o2) {
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

    class SorterPool implements Comparator<PoolWritable> {

        @Override
        public int compare(PoolWritable o1, PoolWritable o2) {
            int comp = o1.query_id - o2.query_id;
            if (comp == 0) {
                comp = o1.update_id.compareTo(o2.update_id);
                if (comp == 0) {
                    comp = o1.update_id.compareTo(o2.update_id);
                }
            }
            return comp;
        }

    }

    public HashMap<String, PoolWritable> getPool(Datafile in) {
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

    public HashMap<String, HashMap<String, MatchEditWritable>> getEmatches(ArrayList<Datafile> ins, HashMap<String, PoolWritable> ePool) {
        HashMap<String, HashMap<String, MatchEditWritable>> results = new HashMap();
        for (Datafile in : ins) {
            MatchFile pf = new MatchFile(in);
            for (MatchWritable w : pf) {
                log.info("%s %s", w.update_id, w.nugget_id);
                //String id = w.update_id + w.nugget_id;
                HashMap<String, MatchEditWritable> list = results.get(w.update_id);
                if (list == null) {
                    list = new HashMap();
                    results.put(w.update_id, list);
                }
                //MatchEditWritable existing = list.get(w.nugget_id);
                MatchEditWritable mw = new MatchEditWritable();
                mw.nugget_id = w.nugget_id;
                mw.query_id = w.query_id;
                mw.update_id = w.update_id;
                if (ePool.containsKey(w.update_id)) {
                    String text = ePool.get(w.update_id).update_text;
                    mw.match = text.substring(w.match_start, Math.min(w.match_end + 1, text.length()));
                } else {
                    log.info("missing %s", w.update_id);
                }
                list.put(w.nugget_id, mw);
            }
        }
        return results;
    }

//    public static void main(String ... args) {
//        ArgsParser ap = new ArgsParser(args, "createpool creatematch existingpool newpool {existingmatch}");
//        Datafile createpool = new Datafile(ap.get("createpool"));
//        Datafile creatematch = new Datafile(ap.get("creatematch"));
//        Datafile existingpool = new Datafile(ap.get("existingpool"));
//        Datafile newpool = new Datafile(ap.get("existingpool"));
//        ArrayList<Datafile> inematch = new ArrayList();
//        if (ap.exists("existingmatch")) {
//            for (String s : ap.getStrings("existingmatch")) {
//                inematch.add(new Datafile(s));
//            }
//        }
//        new CreatePoolFile(createpool, creatematch, existingpool, newpool, inematch);
//    }
}
