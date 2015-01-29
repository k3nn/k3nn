package wp;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 *
 * @author jeroen
 */
public class LinkWritable extends WritableComparable<LinkWritable, LinkFile> {
    public String entity;
    public String anchortext;
    public int frequency;

    public LinkWritable() {
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(entity.hashCode(), anchortext.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LinkWritable) {
           LinkWritable oo = (LinkWritable) o;
           return oo.entity.equals(entity) && oo.anchortext.equals(anchortext);
        }
        return false;
    }

    @Override
    public void read(LinkFile f) {
        this.entity = f.entity.get();
        this.anchortext = f.anchortext.get();
        this.frequency = f.frequency.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write0(entity);
        writer.write0(anchortext);
        writer.write(frequency);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        entity = reader.readString0();
        anchortext = reader.readString0();
        frequency = reader.readInt();
    }

    @Override
    public void write(LinkFile file) {
        file.entity.set(entity);
        file.anchortext.set(anchortext);
        file.frequency.set(frequency);
        file.write();
    }

    @Override
    public int compareTo(LinkWritable o) {
        int comp = o.entity.compareTo(entity);
        if (comp == 0) {
            comp = o.anchortext.compareTo(anchortext);
        }
        return comp;
    }
}
