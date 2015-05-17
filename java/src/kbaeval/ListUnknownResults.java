package kbaeval;

import MatchingClusterNode.MatchingClusterNodeFile;
import MatchingClusterNode.MatchingClusterNodeWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

public class ListUnknownResults {

    private static final Log log = new Log(ListUnknownResults.class);
    MatchingClusterNodeWritable recordcluster = new MatchingClusterNodeWritable();
    TrecFile clusterfile;

    public ListUnknownResults(Datafile in, Datafile inepool) {
        HashMap<String, PoolWritable> ePool = getEPool(inepool);
        //log.info(ematches);
        if (!in.exists())
            in = new Datafile(new Configuration(), in.getCanonicalPath());
        clusterfile = new TrecFile(in);
        HashMap<String, PoolWritable> pooled = new HashMap();
        int count = 0;
        int exist = 0;
        for (TrecWritable w : clusterfile) {
            count++;
            String update_id = w.document + "-" + w.sentence;
            PoolWritable existingpooled = ePool.get(update_id);
            if (existingpooled == null) {
                log.info("%s", update_id);
                exist++;
            }
        }
        log.info("exist %f", exist / (double)count);
    }

    public HashMap<String, PoolWritable> getEPool(Datafile in) {
        HashMap<String, PoolWritable> results = new HashMap();
        PoolFile pf = new PoolFile(in);
        for (PoolWritable w : pf) {
            if (w.query_id < 11) {
                PoolWritable existing = results.get(w.update_id);
                results.put(w.update_id, w);
            }
        }
        return results;
    }

    public static void main(String args[]) {
        ArgsParser ap = new ArgsParser(args, "-i input existingpool");
        Datafile in = new Datafile(ap.get("input"));
        Datafile inepool = new Datafile(ap.get("existingpool"));
        new ListUnknownResults(in, inepool);
    }
}
