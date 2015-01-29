package kbaeval;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.xml.File;
import java.util.HashMap;

/**
 *
 * @author jeroen
 */
public class TopicFile extends File<TopicWritable> {
    public FolderNode event = getRoot();
    public IntField id = this.addInt(event, "id");
    public StringField title = this.addString(event, "title");
    public StringField description = this.addString(event, "description");
    public LongField start = this.addLong(event, "start");
    public LongField end = this.addLong(event, "end");
    public StringField query = this.addString(event, "query");
    public StringField type = this.addString(event, "type");

    public TopicFile(Datafile df) {
        super(df);
    }

    @Override
    public TopicWritable newRecord() {
        return new TopicWritable();
    }  

    @Override
    public FolderNode createRoot() {
        return addNode(null, "event");
    }
    
    public HashMap<Integer, TopicWritable> getMap() {
        HashMap<Integer, TopicWritable> map = new HashMap();
        for (TopicWritable w : this) {
            map.put(w.id, w);
        }
        return map;
    }
}
