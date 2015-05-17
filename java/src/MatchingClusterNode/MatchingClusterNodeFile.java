package MatchingClusterNode;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 * Is basically an extension of ClusterNodeFile, only used for the nodes in 
 * "query matching clusters", i.e. clusters that contain a node with all
 * terms of a query. To allow writing final results, the data is expanded 
 * with docuemnetID and sentence number.
 * @author jeroen
 */
public class MatchingClusterNodeFile extends File<MatchingClusterNodeWritable> {

    // internal cluster ID, -1 if not assigned
    public IntField clusterID = addInt("clusterid");
    // internal sentence ID, which is also used as node ID
    public LongField sentenceID = addLong("sentenceid");
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public IntField domain = addInt("domain");
    // publication/crawl time of the original document 
    public LongField creationtime = addLong("creationtime");
    // often a sentence extracted from a document in the collection
    public StringField content = addString("content");
    // collection ID of the original document
    public StringField documentID = addString("documentid");
    // sentence nr of the content in the original document
    public IntField sentenceNumber = addInt("row");
    // internal node ID's of max K nearest neighbors
    public StringField nnid = addString("nnid");
    // similarity scores of NN
    public StringField nnscore = addString("nnscore");

    public MatchingClusterNodeFile(Datafile df) {
        super(df);
    }

    @Override
    public MatchingClusterNodeWritable newRecord() {
        return new MatchingClusterNodeWritable();
    }  
}
