package kbaeval;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.tsv.Writable;
/**
 *
 * @author jeroen
 */
public class TrecWritable extends Writable<TrecFile> {
    public int topic;
    public String team = "team";
    public String run = "run";
    public String document;
    public int sentence = 0;
    public long timestamp;
    public double confidence = 1.0;

    @Override
    public void read(TrecFile f) {
        this.topic = f.topic.get();
        this.team = f.team.get();
        this.run = f.run.get();
        this.document = f.document.get();
        this.sentence = f.sentence.get();
        this.timestamp = f.timestamp.get();
        this.confidence = f.confidence.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(topic);
        writer.write(team);
        writer.write(run);
        writer.write(document);
        writer.write(sentence);
        writer.write(timestamp);
        writer.write(confidence);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
       topic = reader.readInt();
       team = reader.readString();
       run = reader.readString();
       document = reader.readString();
       sentence = reader.readInt();
       timestamp = reader.readLong();
       confidence = reader.readDouble();
    }

    @Override
    public void write(TrecFile file) {
        file.topic.set(topic);
        file.team.set(team);
        file.run.set(run);
        file.document.set(document);
        file.sentence.set(sentence);
        file.timestamp.set(timestamp);
        file.confidence.set(confidence);
        file.write();
    }
}
