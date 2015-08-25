package kbaeval;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class TrecFile extends File<TrecWritable> {
    IntField topic = this.addInt("topic");
    StringField team = this.addString("team");
    StringField run = this.addString("run");
    StringField document = this.addString("document");
    IntField sentence = this.addInt("sentence");
    LongField timestamp = this.addLong("timestamp");
    DoubleField confidence = this.addDouble("confidence");

    public TrecFile(Datafile df) {
        super(df);
    }

    @Override
    public TrecWritable newRecord() {
        return new TrecWritable();
    }  
}
