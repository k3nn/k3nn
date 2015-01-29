package RelCluster;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class RelClusterWritable extends Writable<RelClusterFile> {
    public int clusterid;
    public int urlid;
    public long creationtime;
    public String title;
    public int domain;
    public String documentid;
    public int row;
    public String nnid;
    public String nnscore;
    
    @Override
    public void read(RelClusterFile f) {
        this.clusterid = f.clusterid.get();
        this.urlid = f.urlid.get();
        this.domain = f.domain.get();
        this.creationtime = f.creationtime.get();
        this.title = f.title.get();
        this.documentid = f.documentid.get();
        this.row = f.row.get();
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
        writer.write(documentid);
        writer.write(row);
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
        documentid = reader.readString();
        row = reader.readInt();
        nnid = reader.readString();
        nnscore = reader.readString();
    }

    @Override
    public void write(RelClusterFile file) {
        file.clusterid.set(clusterid);
        file.urlid.set(urlid);
        file.domain.set(domain);
        file.creationtime.set(creationtime);
        file.title.set(title);
        file.documentid.set(documentid);
        file.row.set(row);
        file.nnid.set(nnid);
        file.nnscore.set(nnscore);
        file.write();
    }
}
