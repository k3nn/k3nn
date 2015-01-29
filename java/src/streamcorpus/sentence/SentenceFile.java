package streamcorpus.sentence;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class SentenceFile extends File<SentenceWritable> {

    public IntField id = addInt("id");
    public LongField uuidlow = addLong("uuidlow");
    public LongField uuidhigh = addLong("uuidhigh");
    public LongField creationtime = addLong("creationtime");
    public IntField domain = addInt("domain");
    public IntField row = addInt("row");
    public StringField sentence = this.addString("sentence");

    public SentenceFile(Datafile df) {
        super(df);
    }

    @Override
    public SentenceWritable newRecord() {
        return new SentenceWritable();
    }  
}
