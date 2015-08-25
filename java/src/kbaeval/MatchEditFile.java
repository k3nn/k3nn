package kbaeval;

import kbaeval.*;
import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class MatchEditFile extends File<MatchEditWritable> {
    IntField query_id = this.addInt("query_id");
    StringField update_id = this.addString("update_id");
    StringField nugget_id = this.addString("nugget_id");
    StringField match = this.addString("match");

    public MatchEditFile(Datafile df) {
        super(df);
        this.hasHeader();
    }

    @Override
    public MatchEditWritable newRecord() {
        return new MatchEditWritable();
    }  
}
