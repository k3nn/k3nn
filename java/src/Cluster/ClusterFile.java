package Cluster;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class ClusterFile extends File<ClusterWritable> {

    public IntField clusterid = addInt("clusterid");
    public IntField urlid = addInt("urlid");
    public IntField domain = addInt("domain");
    public LongField creationtime = addLong("creationtime");
    public StringField title = addString("title");
    public StringField nnid = addString("nnid");
    public StringField nnscore = addString("nnscore");

    public ClusterFile(Datafile df) {
        super(df);
    }

    @Override
    public ClusterWritable newRecord() {
        return new ClusterWritable();
    }  
}
