package scrape.date;

import scrape.article.*;
import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class SnapshotWritable extends Writable<SnapshotFile> {
    public String creationtime;
    public String domain;
    public String url;

    public SnapshotWritable() {
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(domain.hashCode(), url.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SnapshotWritable) {
           SnapshotWritable oo = (SnapshotWritable) o;
           return oo.url.equals(url) && oo.domain.equals(domain);
        }
        return false;
    }

    @Override
    public void read(SnapshotFile f) {
        this.creationtime = f.creationtime.get();
        this.domain = f.domain.get();
        this.url = f.url.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(domain);
        writer.write(url);
        writer.write(creationtime);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        domain = reader.readString();
        url = reader.readString();
        creationtime = reader.readString();
    }

    @Override
    public void write(SnapshotFile file) {
        file.creationtime.set(creationtime);
        file.domain.set(domain);
        file.url.set(url);
        file.write();
    }
}
