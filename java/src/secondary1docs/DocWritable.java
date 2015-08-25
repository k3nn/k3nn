package secondary1docs;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class DocWritable extends Writable<DocFile> {
    public String docid;
    public long emit;
    
    @Override
    public void read(DocFile f) {
        this.docid = f.docid.get();
        this.emit = f.emittime.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(docid);
        writer.write(emit);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        docid = reader.readString();
        emit = reader.readLong();
    }

    @Override
    public void write(DocFile file) {
        file.docid.set(docid);
        file.emittime.set(emit);
        file.write();
    }
}
