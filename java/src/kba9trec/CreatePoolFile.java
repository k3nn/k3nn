package kba9trec;

import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.StrTools;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class CreatePoolFile {

    private static final Log log = new Log(CreatePoolFile.class);
    RelClusterWritable recordcluster = new RelClusterWritable();
    RelClusterFile clusterfile;

    public CreatePoolFile(Datafile in, Datafile pool, Datafile matchfile, Datafile inepool, ArrayList<Datafile> inematch) {
        HashMap<String, PoolWritable> ePool = getEPool(inepool);
        //log.info(ematches);
        PoolFile poolfile = new PoolFile(pool);
        poolfile.openWrite();
        ArrayList<MatchEditWritable> matched = new ArrayList();
        clusterfile = new RelClusterFile(in);
        HashMap<String, PoolWritable> pooled = new HashMap();
        for (RelClusterWritable w : clusterfile) {
            PoolWritable record = new PoolWritable();
            record.update_id = w.documentid + "-" + w.row;
            record.query_id = w.clusterid;
            PoolWritable existingpooled = ePool.get(record.update_id);
            if (existingpooled != null && existingpooled.query_id == record.query_id) {
                existingpooled.write(poolfile);
                pooled.put(existingpooled.update_id, existingpooled);
            } else {
                record.doc_id = w.documentid;
                record.sentence_id = w.row;
                record.update_id = w.documentid + "-" + w.row;
                record.update_len = StrTools.countIndexOf(w.title, ' ') + 2;
                record.update_text = w.title;
                record.write(poolfile);
                pooled.put(record.update_id, record);
            }
        }

        HashMap<String, HashMap<String, MatchEditWritable>> ematches = getEmatches(inematch, pooled, ePool, poolfile);
        for (PoolWritable record : pooled.values()) {
            HashMap<String, MatchEditWritable> list = ematches.get(record.update_id);
            if (list != null) {
                for (MatchEditWritable match : list.values()) {
                    matched.add(match);
                }
            } else {
                MatchEditWritable match = new MatchEditWritable();
                match.query_id = record.query_id;
                match.update_id = record.update_id;
                match.match = record.update_text;
                matched.add(match);
            }
        }
        poolfile.closeWrite();
        Collections.sort(matched, new Sorter());
        MatchEditFile mf = new MatchEditFile(matchfile);
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

    public HashMap<String, HashMap<String, MatchEditWritable>> getEmatches(ArrayList<Datafile> ins, 
            HashMap<String, PoolWritable> updates, HashMap<String, PoolWritable> ePool, PoolFile poolfile) {
        HashMap<String, HashMap<String, MatchEditWritable>> results = new HashMap();
        for (Datafile in : ins) {
            MatchFile pf = new MatchFile(in);
            for (MatchWritable w : pf) {
                log.info("%s %s", w.update_id, w.nugget_id);
                String id = w.update_id + w.nugget_id;
                HashMap<String, MatchEditWritable> list = results.get(w.update_id);
                if (list == null) {
                    list = new HashMap();
                    results.put(w.update_id, list);
                }
                MatchEditWritable existing = list.get(w.nugget_id);
                MatchEditWritable mw = new MatchEditWritable();
                mw.nugget_id = w.nugget_id;
                mw.query_id = w.query_id;
                mw.update_id = w.update_id;
                if (!updates.containsKey(w.update_id)) {
                    PoolWritable existingpooled = ePool.get(w.update_id);
                    if (existingpooled != null && existingpooled.query_id == mw.query_id) {
                       existingpooled.write(poolfile);
                       updates.put(existingpooled.update_id, existingpooled);
                       log.info("add %s", w.update_id);
                    } else {
                        log.info("miss %s", w.update_id);
                    }
                }
                if (updates.containsKey(w.update_id)) {
                    String text = updates.get(w.update_id).update_text;
                    log.info("%s %s %d %d", w.update_id, text, w.match_start, w.match_end);
                    mw.match = text.substring(w.match_start, Math.min(w.match_end + 1, text.length()));
                }  
                list.put(w.nugget_id, mw);
            }
        }
        return results;
    }

    public ArrayMap<Integer, String> getMatchedUpdates(ArrayList<Datafile> ins) {
        ArrayMap<Integer, String> results = new ArrayMap();
        for (Datafile in : ins) {
            MatchFile pf = new MatchFile(in);
            for (MatchWritable w : pf) {
                results.add(w.query_id, w.update_id);
            }
        }
        return results;
    }

    public static void main(String args[]) {
        ArgsParser ap = new ArgsParser(args, "-i input outpool outmatch existingpool {existingmatch}");
        Datafile in = new Datafile(ap.get("input"));
        Datafile out = new Datafile(ap.get("outpool"));
        Datafile outmatch = new Datafile(ap.get("outmatch"));
        Datafile inepool = new Datafile(ap.get("existingpool"));
        ArrayList<Datafile> inematch = new ArrayList();
        if (ap.exists("existingmatch")) {
            for (String s : ap.getStrings("existingmatch")) {
                inematch.add(new Datafile(s));
            }
        }
        new CreatePoolFile(in, out, outmatch, inepool, inematch);
    }
}
