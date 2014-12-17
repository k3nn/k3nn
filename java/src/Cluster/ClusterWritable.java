package Cluster;

import KNN.Url;
import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.Structured.Writable;
import java.util.ArrayList;
/**
 *
 * @author jeroen
 */
public class ClusterWritable extends Writable<ClusterFile> {
    public int clusterid;
    public int urlid;
    public long creationtime;
    public String title;
    public int domain;
    public String nnid;
    public String nnscore;

    public ClusterWritable() {
    }
    
    public int hashCode() {
        return urlid;
    }

    public boolean equals(Object o) {
        return ((ClusterWritable)o).urlid == urlid;
    }
    
    public void set(Url url) {
        this.clusterid = url.isClustered()?url.getCluster().getID():-1;
        this.urlid = url.getID();
        this.domain = url.getDomain();
        this.creationtime = url.getCreationTime();
        this.title = "";
        this.nnid = url.getNN();
        this.nnscore = url.getScore();
    }
    
    @Override
    public void read(ClusterFile f) {
        this.clusterid = f.clusterid.get();
        this.urlid = f.urlid.get();
        this.domain = f.domain.get();
        this.creationtime = f.creationtime.get();
        this.title = f.title.get();
        this.nnid = f.nnid.get();
        this.nnscore = f.nnscore.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(clusterid);
        writer.write(urlid);
        writer.write(domain);
        writer.write(creationtime);
        writer.write(title);
        writer.write(nnid);
        writer.write(nnscore);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        clusterid = reader.readInt();
        urlid = reader.readInt();
        domain = reader.readInt();
        creationtime = reader.readLong();
        title = reader.readString();
        nnid = reader.readString();
        nnscore = reader.readString();
    }

    @Override
    public void write(ClusterFile file) {
        file.clusterid.set(clusterid);
        file.urlid.set(urlid);
        file.domain.set(domain);
        file.creationtime.set(creationtime);
        file.title.set(title);
        file.nnid.set(nnid);
        file.nnscore.set(nnscore);
        file.write();
    }
    
    public ArrayList<Integer> getNN() {
        String parts[] = nnid.split(",");
        ArrayList<Integer> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Integer.parseInt(p));
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
