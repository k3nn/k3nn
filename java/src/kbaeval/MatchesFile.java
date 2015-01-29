package kbaeval;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author jeroen
 */
public class MatchesFile extends File<MatchesWritable> {
    public IntField query = this.addInt("query");
    public StringField updateid = this.addString("updateid");
    public StringField nuggetid = this.addString("nuggetid");
    public IntField start = this.addInt("start");
    public IntField end = this.addInt("end");
    public DoubleField auto = this.addDouble("auto");

    public MatchesFile(Datafile df) {
        super(df);
        this.hasHeader();
    }

    @Override
    public MatchesWritable newRecord() {
        return new MatchesWritable();
    }  

    public HashMap<String, ArrayList<MatchesWritable>> getMap() {
        HashMap<String, ArrayList<MatchesWritable>> map = new HashMap();
        for (MatchesWritable w : this) {
            ArrayList<MatchesWritable> list = map.get(w.updateid);
            if (list == null) {
                list = new ArrayList();
                map.put(w.updateid, list);
            }
            list.add(w);
        }
        return map;
    }
}
