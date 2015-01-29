package kba9trec;

import kbaeval.*;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class MatchFile extends File<MatchWritable> {
    IntField query_id = this.addInt("query_id");
    StringField update_id = this.addString("update_id");
    StringField nugget_id = this.addString("nugget_id");
    IntField match_start = this.addInt("match_start");
    IntField match_end = this.addInt("match_end");
    IntField auto_p = this.addInt("auto_p");

    public MatchFile(Datafile df) {
        super(df);
        this.hasHeader();
    }

    @Override
    public MatchWritable newRecord() {
        return new MatchWritable();
    }  
}
