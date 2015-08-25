package kbaeval;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class DuplicateWritable extends Writable<DuplicateFile> {
    public String duplicate;
    public String original;

    @Override
    public void read(DuplicateFile f) {
        this.duplicate = f.duplicate.get();
        this.original = f.original.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(duplicate);
        writer.write(original);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
       duplicate = reader.readString();
       original = reader.readString();
    }

    @Override
    public void write(DuplicateFile file) {
        file.duplicate.set(duplicate);
        file.original.set(original);
        file.write();
    }
}
