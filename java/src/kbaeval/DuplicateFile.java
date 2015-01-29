package kbaeval;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

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
