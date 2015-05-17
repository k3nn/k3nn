package ClusterNode;

import KNN.Node;
import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.tsv.Writable;
import io.github.repir.tools.lib.MathTools;
import java.util.ArrayList;

/**
 * Contains a single node, with nearest neighbor assignments and clusterid.
 * @author jeroen
 */
public class ClusterNodeWritable extends Writable<ClusterNodeFile> {
    // internal cluster ID, -1 if not assigned
    public int clusterID;
    // internal sentenceID, which is also used as nodeID
    public long sentenceID;
    // publication/crawl time of the original document 
    public long creationTime;
    
    public String content;
    // domain corrsponding to domain list in resources, from the url it was taken
    public int domain;
    // id's of max K nearest neighbors
    public String nnid;
    // similarity scores of NN
    public String nnscore;

    public ClusterNodeWritable() {
    }
    
    public int hashCode() {
        return MathTools.hashCode(sentenceID);
    }

    public boolean equals(Object o) {
        return ((ClusterNodeWritable)o).sentenceID == sentenceID;
    }
    
    public void set(Node url) {
        this.clusterID = url.isClustered()?url.getCluster().getID():-1;
        this.sentenceID = url.getID();
        this.domain = url.getDomain();
        this.creationTime = url.getCreationTime();
        this.content = "";
        this.nnid = url.getNN();
        this.nnscore = url.getScore();
    }
    
    public void setRemovedCluster(int id, long creationtime) {
        this.clusterID = id;
        this.sentenceID = -1;
        this.domain = -1;
        this.creationTime = creationtime;
        this.content = "";
        this.nnid = "";
        this.nnscore = "";
    }
    
    @Override
    public void read(ClusterNodeFile f) {
        this.clusterID = f.clusterID.get();
        this.sentenceID = f.sentenceID.get();
        this.domain = f.domain.get();
        this.creationTime = f.creationtime.get();
        this.content = f.content.get();
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
        writer.write(nnid);
        writer.write(nnscore);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        clusterID = reader.readInt();
        sentenceID = reader.readInt();
        domain = reader.readInt();
        creationTime = reader.readLong();
        content = reader.readString();
        nnid = reader.readString();
        nnscore = reader.readString();
    }

    @Override
    public void write(ClusterNodeFile file) {
        file.clusterID.set(clusterID);
        file.sentenceID.set(sentenceID);
        file.domain.set(domain);
        file.creationtime.set(creationTime);
        file.content.set(content);
        file.nnid.set(nnid);
        file.nnscore.set(nnscore);
        file.write();
    }
    
    public ArrayList<Long> getNN() {
        String parts[] = nnid.split(",");
        ArrayList<Long> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Long.parseLong(p));
            }
        }
        return list;
    }

    public ArrayList<Double> getNNScore() {
        String parts[] = nnscore.split(",");
        ArrayList<Double> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Double.parseDouble(p));
            }
        }
        return list;
    }
    
}
