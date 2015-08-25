package MatchingClusterNode;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.hadoop.tsv.Writable;

/**
 * Is basically an extension of ClusterNodeWritable, only used for the nodes in 
 * "query matching clusters", i.e. clusters that contain a node with all
 * terms of a query. To allow writing final results, the data is expanded 
 * with docuemnetID and sentence number.
 * @author jeroen
 */
public class MatchingClusterNodeWritable extends Writable<MatchingClusterNodeFile> {
    // internal cluster ID, -1 if not assigned
    public int clusterID;
    // internal sentence ID, which is also used as node ID
    public long sentenceID;
    // publication/crawl time of the original document 
    public long creationTime;
    // often a sentence extracted from a document in the collection
    public String content;
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public int domain;
    // collection ID of the original document
    public String documentID;
    // sentence nr of the content in the original document
    public int sentenceNumber;
    // internal node ID's of max K nearest neighbors
    public String nnid;
    // similarity scores of NN
    public String nnscore;
    
    @Override
    public void read(MatchingClusterNodeFile f) {
        this.clusterID = f.clusterID.get();
        this.sentenceID = f.sentenceID.get();
        this.domain = f.domain.get();
        this.creationTime = f.creationtime.get();
        this.content = f.content.get();
        this.documentID = f.documentID.get();
        this.sentenceNumber = f.sentenceNumber.get();
        this.nnid = f.nnid.get();
        this.nnscore = f.nnscore.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(clusterID);
        writer.write(sentenceID);
        writer.write(domain);
        writer.write(creationTime);
        writer.write(content);
        writer.write(documentID);
        writer.write(sentenceNumber);
        writer.write(nnid);
        writer.write(nnscore);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        clusterID = reader.readInt();
        sentenceID = reader.readLong();
        domain = reader.readInt();
        creationTime = reader.readLong();
        content = reader.readString();
        documentID = reader.readString();
        sentenceNumber = reader.readInt();
        nnid = reader.readString();
        nnscore = reader.readString();
    }

    @Override
    public void write(MatchingClusterNodeFile file) {
        file.clusterID.set(clusterID);
        file.sentenceID.set(sentenceID);
        file.domain.set(domain);
        file.creationtime.set(creationTime);
        file.content.set(content);
        file.documentID.set(documentID);
        file.sentenceNumber.set(sentenceNumber);
        file.nnid.set(nnid);
        file.nnscore.set(nnscore);
        file.write();
    }
}
