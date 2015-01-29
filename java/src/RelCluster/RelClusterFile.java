package RelCluster;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class RelClusterFile extends File<RelClusterWritable> {

    public IntField clusterid = addInt("clusterid");
    public IntField urlid = addInt("urlid");
    public IntField domain = addInt("domain");
    public LongField creationtime = addLong("creationtime");
    public StringField title = addString("title");
    public StringField documentid = addString("documentid");
    public IntField row = addInt("row");
    public StringField nnid = addString("nnid");
    public StringField nnscore = addString("nnscore");

    public RelClusterFile(Datafile df) {
        super(df);
    }

    @Override
    public RelClusterWritable newRecord() {
        return new RelClusterWritable();
    }  
}
