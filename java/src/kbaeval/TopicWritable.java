package kbaeval;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.lib.MathTools;
import io.github.repir.tools.hadoop.xml.Writable;
/**
 *
 * @author jeroen
 */
public class TopicWritable extends Writable<TopicFile> {
    public int id;
    public String title;
    public String description;
    public long start;
    public long end;
    public String query;
    public String type;

    public TopicWritable() {
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TopicWritable) {
           TopicWritable oo = (TopicWritable) o;
           return oo.id == id;
        }
        return false;
    }

    @Override
    public void read(TopicFile f) {
        this.id = f.id.get();
        this.title = f.title.get();
        this.description = f.description.get();
        this.start = f.start.get();
        this.end = f.end.get();
        this.query = f.query.get();
        this.type = f.type.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(id);
        writer.write(title);
        writer.write(description);
        writer.write(start);
        writer.write(end);
        writer.write(query);
        writer.write(type);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readInt();
        title = reader.readString();
        description = reader.readString();
        start = reader.readLong();
        end = reader.readLong();
        query = reader.readString();
        type = reader.readString();
    }

    @Override
    public void write(TopicFile file) {
        file.id.set(id);
        file.title.set(title);
        file.description.set(description);
        file.start.set(start);
        file.end.set(end);
        file.query.set(query);
        file.type.set(type);
        file.write();
    }
}
