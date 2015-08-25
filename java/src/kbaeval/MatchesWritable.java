package kbaeval;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class MatchesWritable extends Writable<MatchesFile> {
    public int queryid;
    public String updateid;
    public String nuggetid;
    public int start;
    public int end;
    public double auto;

    public MatchesWritable() {
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(queryid);
        writer.write(updateid);
        writer.write(nuggetid);
        writer.write(start);
        writer.write(end);
        writer.write(auto);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        queryid = reader.readInt();
        updateid = reader.readString();
        nuggetid = reader.readString();
        start = reader.readInt();
        end = reader.readInt();
        auto = reader.readDouble();
    }

    @Override
    public void write(MatchesFile file) {
        file.query.set(queryid);
        file.updateid.set(updateid);
        file.nuggetid.set(nuggetid);
        file.start.set(start);
        file.end.set(end);
        file.auto.set(auto);
        file.write();
    }

    @Override
    public void read(MatchesFile file) {
        queryid = file.query.get();
        updateid = file.updateid.get();
        nuggetid = file.nuggetid.get();
        start = file.start.get();
        end = file.end.get();
        auto = file.auto.get();
    }
}
