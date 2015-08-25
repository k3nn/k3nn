package Cluster;

import com.google.gson.reflect.TypeToken;
import io.github.htools.io.Datafile;
import io.github.htools.hadoop.json.File;
import java.util.ArrayList;

/**
 * Stores clusters of nodes.
 * @author jeroen
 */
public class ClusterFile extends File<ClusterWritable> {

    public IntField clusterid = addInt("clusterid");
    public JsonArrayField<NodeWritable> nodes = 
            this.addJsonArray("nodes", new TypeToken<ArrayList<NodeWritable>>(){}.getType());
    
    public ClusterFile(Datafile df) {
        super(df);
    }

    @Override
    public ClusterWritable newRecord() {
        return new ClusterWritable();
    }  
}
