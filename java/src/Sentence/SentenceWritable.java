package Sentence;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.MathTools;
import io.github.htools.hadoop.tsv.Writable;
import java.util.UUID;

/**
 * A sentence from the (KBA) collection.
 * @author jeroen
 */
public class SentenceWritable extends Writable<SentenceFile> {
    // internal sentence ID, which is also used as node ID in clustering
    public long sentenceID;
    // long representation of the UUID used as a collection document ID
    public long documentIDLow;
    public long documentIDHigh;
    // publication/crawl time of document
    public long creationtime;
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public int domain;
    // often a sentence extracted from a document in the collection
    public String content;
    // sentence nr of the content in the original document, i.e. the sentence 
    // number assigned to the sentence in the KBA corpus. 
    public int sentenceNumber;

    public SentenceWritable() {
    }
    
    /**
     * Sets the document's collection UUID 
     * @param uuidstring 
     */
    public void setUUID(String uuidstring) {
        UUID uuid = UUID.fromString(uuidstring);
        documentIDHigh = uuid.getMostSignificantBits();
        documentIDLow = uuid.getLeastSignificantBits();
    }
    
    public SentenceWritable clone() {
        SentenceWritable s = new SentenceWritable();
        s.content = content;
        s.creationtime = creationtime;
        s.documentIDHigh = this.documentIDHigh;
        s.documentIDLow = this.documentIDLow;
        s.domain = domain;
        s.sentenceID = sentenceID;
        s.sentenceNumber = sentenceNumber;
        return s;
    }
    
    /**
     * @return UUID of document in the collection
     */
    public UUID getUUID() {
        return new UUID(documentIDHigh, documentIDLow);
    }
    
    public long getDay() {
        return sentenceID >> 22;
    }
    
    public void setID(long day, int sequence) {
        sentenceID = (day << 22) + sequence;
    }
    
    /**
     * 
     * @return reconstructed collection document ID, consisting of timestamp
     * and UUID
     */
    public String getDocumentID() {
        return creationtime + "-" + getUUID().toString().replace("-", "");
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(sentenceID);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SentenceWritable) {
           SentenceWritable oo = (SentenceWritable) o;
           return oo.sentenceID == sentenceID;
        }
        return false;
    }

    @Override
    public void read(SentenceFile f) {
        this.sentenceID = f.sentenceID.get();
        this.documentIDHigh = f.documentUUIDHigh.get();
        this.documentIDLow = f.documentUUIDLow.get();
        this.creationtime = f.creationTime.get();
        this.content = f.content.get();
        this.sentenceNumber = f.sentenceNumber.get();
        this.domain = f.domain.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(sentenceID);
        writer.write(documentIDLow);
        writer.write(documentIDHigh);
        writer.write(domain);
        writer.write(sentenceNumber);
        writer.write(content);
        writer.write(creationtime);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        sentenceID = reader.readLong();
        documentIDLow = reader.readLong();
        documentIDHigh = reader.readLong();
        domain = reader.readInt();
        sentenceNumber = reader.readInt();
        content = reader.readString();
        creationtime = reader.readLong();
    }

    @Override
    public void write(SentenceFile file) {
        file.sentenceID.set(sentenceID);
        file.documentUUIDLow.set(documentIDLow);
        file.documentUUIDHigh.set(documentIDHigh);
        file.domain.set(domain);
        file.creationTime.set(creationtime);
        file.sentenceNumber.set(sentenceNumber);
        file.content.set(content);
        file.write();
    }
}
