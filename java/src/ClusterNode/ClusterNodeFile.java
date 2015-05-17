package ClusterNode;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 * Stores nodes with their assigned cluster
 * @author jeroen
 */
public class ClusterNodeFile extends File<ClusterNodeWritable> {

    // internal cluster ID, -1 if not assigned
    public IntField clusterID = addInt("clusterid");
    // internal sentenceID, which is also used as nodeID
    public LongField sentenceID = addLong("sentenceID");
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public IntField domain = addInt("domain"); 
    // publication/crawl time of the original document 
    public LongField creationtime = addLong("creationtime");

    public StringField content = addString("content");
    // internal node ID's of max K nearest neighbors
    public StringField nnid = addString("nnid");
    // similarity scores of NN
    public StringField nnscore = addString("nnscore");

    public ClusterNodeFile(Datafile df) {
        super(df);
    }

    @Override
    public ClusterNodeWritable newRecord() {
        return new ClusterNodeWritable();
    }  
}
