package kbaeval;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class DuplicateFile extends File<DuplicateWritable> {
    StringField duplicate = this.addString("duplicate");
    StringField original = this.addString("original");

    public DuplicateFile(Datafile df) {
        super(df);
    }

    @Override
    public DuplicateWritable newRecord() {
        return new DuplicateWritable();
    }  
}
