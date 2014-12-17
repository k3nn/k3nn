package streamcorpus.sentence;

import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.hadoop.Structured.Writable;
import java.util.UUID;
/**
 *
 * @author jeroen
 */
public class SentenceWritable extends Writable<SentenceFile> {
    public int id;
    public long idlow;
    public long idhigh;
    public long creationtime;
    public int domain;
    public String sentence;
    public int row;

    public SentenceWritable() {
    }

    public void setUUID(String uuidstring) {
        UUID uuid = UUID.fromString(uuidstring);
        idhigh = uuid.getMostSignificantBits();
        idlow = uuid.getLeastSignificantBits();
    }
    
    public UUID getUUID() {
        return new UUID(idhigh, idlow);
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SentenceWritable) {
           SentenceWritable oo = (SentenceWritable) o;
           return oo.id == id;
        }
        return false;
    }

    @Override
    public void read(SentenceFile f) {
        this.id = f.id.get();
        this.idhigh = f.uuidhigh.get();
        this.idlow = f.uuidlow.get();
        this.creationtime = f.creationtime.get();
        this.sentence = f.sentence.get();
        this.row = f.row.get();
        this.domain = f.domain.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(id);
        writer.write(idlow);
        writer.write(idhigh);
        writer.write(domain);
        writer.write(row);
        writer.write(sentence);
        writer.write(creationtime);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        id = reader.readInt();
        idlow = reader.readLong();
        idhigh = reader.readLong();
        domain = reader.readInt();
        row = reader.readInt();
        sentence = reader.readString();
        creationtime = reader.readLong();
    }

    @Override
    public void write(SentenceFile file) {
        file.id.set(id);
        file.uuidlow.set(idlow);
        file.uuidhigh.set(idhigh);
        file.domain.set(domain);
        file.creationtime.set(creationtime);
        file.row.set(row);
        file.sentence.set(sentence);
        file.write();
    }
}
