package StreamCluster;

import com.google.gson.reflect.TypeToken;
import io.github.repir.tools.hadoop.json.Writable;
import java.lang.reflect.Type;
import java.util.ArrayList;
/**
 *
 * @author jeroen
 */
public class StreamClusterWritable extends Writable<StreamClusterFile> {
    public static Type type = new TypeToken<StreamClusterWritable>(){}.getType();
    public int clusterid;
    public ArrayList<UrlWritable> urls = new ArrayList();

    public StreamClusterWritable() {
    }
    
    @Override
    public void read(StreamClusterFile f) {
        this.clusterid = f.clusterid.get();
        this.urls = f.urls.get();
    }

    @Override
    public void write(StreamClusterFile file) {
        file.clusterid.set(clusterid);
        file.urls.set(urls);
        file.write();
    }
    
    public UrlWritable lastUrl() {
        return urls.get((urls.size()-1));
    }
    
    @Override
    protected Type getType() {
        return type;
    }

    @Override
    protected void getAttributes(Object o) {
        if (o instanceof StreamClusterWritable) {
            StreamClusterWritable u = (StreamClusterWritable) o;
            this.clusterid = u.clusterid;
            this.urls = u.urls;
        }
    }
    
}
