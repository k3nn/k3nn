package kba6finalcluster;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import gnu.trove.set.hash.TIntHashSet;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.io.IntIntWritable;
import io.github.repir.tools.hadoop.LogFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class FinalMap extends Mapper<NullWritable, NullWritable, IntIntWritable, ClusterWritable> {

    public static final Log log = new Log(FinalMap.class);
    Configuration conf;
    int currentcluster = -1;
    boolean clusterintact;
    public IntIntWritable outkey = new IntIntWritable();
    ArrayList<ClusterWritable> list = new ArrayList();
    TIntHashSet urls = new TIntHashSet();
    TIntHashSet clusters = new TIntHashSet();
    HDFSPath path;

    @Override
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        path = new HDFSPath(conf, conf.get("input"));
        if (!path.exists())
            path = path.getParent();
        conf = context.getConfiguration();
    }

    @Override
    public void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        ArrayList<String> dates = getDates(conf);
        for (int i = dates.size() - 1; i >= 0; i--) {
            outkey.set(i);
            log.info("date %s", dates.get(i));
            ClusterFile cf = new ClusterFile(path.getFile(dates.get(i)));
            cf.openRead();
            for (ClusterWritable cw : cf) {
                map(cw, context);
            }
            cf.closeRead();
        }
    }

    public void map(ClusterWritable value, Context context) throws IOException, InterruptedException {
        if (currentcluster > -1 && currentcluster != value.clusterid) {
            cleanup(context);
            currentcluster = -1;
            list = new ArrayList();
            clusterintact = false;
        }
        if (value.clusterid >= 0) {
            if (currentcluster < 0) {
                currentcluster = value.clusterid;
                clusterintact = !clusters.contains(currentcluster);
            }
            if (urls.contains(value.urlid)) {
                clusterintact = false;
            } else {
                list.add(value);
            }
        } else {
            if (!urls.contains(value.urlid)) {
                outkey.set(outkey.get(), Integer.MAX_VALUE);
                context.write(outkey, value);
                urls.add(value.urlid);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (clusterintact) {
            outkey.set(outkey.get(), currentcluster);
            for (ClusterWritable u : list) {
                context.write(outkey, u);
                urls.add(u.urlid);
            }
        } else {
            outkey.set(outkey.get(), -1);
            for (ClusterWritable u : list) {
                if (!urls.contains(u.urlid)) {
                    u.clusterid = -1;
                    context.write(outkey, u);
                    urls.add(u.urlid);
                }
            }
        }
        clusters.add(currentcluster);
    }

    public static ArrayList<String> getDates(Configuration conf) throws IOException {
        HDFSPath path = new HDFSPath(conf, conf.get("input"));
        if (path.exists())
           return path.getFilenames();
        else {
           String file = path.getName();
           path = path.getParent();
           return path.getFilenames(file);
        }
            
    }
}
