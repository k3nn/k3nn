package kbaeval;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class PoolWritable extends Writable<PoolFile> {
    public int query_id;
    public String update_id;
    public String doc_id;
    public int sentence_id;
    public int update_len;
    public String duplicate_id = "NULL";
    public String update_text;

    @Override
    public void read(PoolFile f) {
        this.query_id = f.query_id.get();
        this.update_id = f.update_id.get();
        this.doc_id = f.doc_id.get();
        this.sentence_id = f.sentence_id.get();
        this.update_len = f.update_len.get();
        this.duplicate_id = f.duplicate_id.get();
        this.update_text = f.update_text.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(query_id);
        writer.write(update_id);
        writer.write(doc_id);
        writer.write(sentence_id);
        writer.write(update_len);
        writer.write(duplicate_id);
        writer.write(update_text);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
       query_id = reader.readInt();
       update_id = reader.readString();
       doc_id = reader.readString();
       sentence_id = reader.readInt();
       update_len = reader.readInt();
       duplicate_id = reader.readString();
       update_text = reader.readString();
    }

    @Override
    public void write(PoolFile file) {
        file.query_id.set(query_id);
        file.update_id.set(update_id);
        file.doc_id.set(doc_id);
        file.sentence_id.set(sentence_id);
        file.update_len.set(update_len);
        file.duplicate_id.set(duplicate_id);
        file.update_text.set(update_text);
        file.write();
    }
}
