package StreamCluster;

import com.google.gson.reflect.TypeToken;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.json.File;
import java.util.ArrayList;

/**
 *
 * @author jeroen
 */
public class StreamClusterFile extends File<StreamClusterWritable> {

    public IntField clusterid = addInt("clusterid");
    public JsonArrayField<UrlWritable> urls = 
            this.addJsonArray("urls", new TypeToken<ArrayList<UrlWritable>>(){}.getType());
    
    public StreamClusterFile(Datafile df) {
        super(df);
    }

    @Override
    public StreamClusterWritable newRecord() {
        return new StreamClusterWritable();
    }  
}
