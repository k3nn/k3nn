package Cluster;

import com.google.gson.reflect.TypeToken;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.json.File;
import java.util.ArrayList;

/**
 *
 * @author jeroen
 */
public class ClusterFile extends File<ClusterWritable> {

    public IntField clusterid = addInt("clusterid");
    public JsonArrayField<NodeWritable> urls = 
            this.addJsonArray("urls", new TypeToken<ArrayList<NodeWritable>>(){}.getType());
    
    public ClusterFile(Datafile df) {
        super(df);
    }

    @Override
    public ClusterWritable newRecord() {
        return new ClusterWritable();
    }  
}
