package Sentence;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class SentenceFile extends File<SentenceWritable> {

    // internal sentence ID, which is also used as node ID in clustering
    public LongField sentenceID = addLong("sentenceid");
    public LongField documentUUIDLow = addLong("uuidlow");
    public LongField documentUUIDHigh = addLong("uuidhigh");
    // publication/crawl time of the original document 
    public LongField creationTime = addLong("creationtime");
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public IntField domain = addInt("domain");
    // sentence nr of the content in the original document
    public IntField sentenceNumber = addInt("sentencenumber");
    // often a sentence extracted from a document in the collection
    public StringField content = this.addString("content");

    public SentenceFile(Datafile df) {
        super(df);
    }

    @Override
    public SentenceWritable newRecord() {
        return new SentenceWritable();
    }  
}
