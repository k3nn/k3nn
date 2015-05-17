package kbaeval;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class MatchWritable extends Writable<MatchFile> {
    public int query_id;
    public String update_id;
    public String nugget_id;
    public int match_start;
    public int match_end;
    public int auto_p = 0;

    @Override
    public void read(MatchFile f) {
        this.query_id = f.query_id.get();
        this.update_id = f.update_id.get();
        this.nugget_id = f.nugget_id.get();
        this.match_start = f.match_start.get();
        this.match_end = f.match_end.get();
        this.auto_p = f.auto_p.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(query_id);
        writer.write(update_id);
        writer.write(nugget_id);
        writer.write(match_start);
        writer.write(match_end);
        writer.write(auto_p);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
       query_id = reader.readInt();
       update_id = reader.readString();
       nugget_id = reader.readString();
       match_start = reader.readInt();
       match_end = reader.readInt();
       auto_p = reader.readInt();
    }

    @Override
    public void write(MatchFile file) {
        file.query_id.set(query_id);
        file.update_id.set(update_id);
        file.nugget_id.set(nugget_id);
        file.match_start.set(match_start);
        file.match_end.set(match_end);
        file.auto_p.set(auto_p);
        file.write();
    }
}
