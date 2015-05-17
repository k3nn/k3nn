package Cluster;

import com.google.gson.reflect.TypeToken;
import io.github.repir.tools.hadoop.json.Writable;
import java.lang.reflect.Type;
import java.util.ArrayList;

/**
 * A cluster of nodes, uses gson for communication between map/reduce.
 * @author jeroen
 */
public class ClusterWritable extends Writable<ClusterFile> {
    // needed for gson
    public static Type type = new TypeToken<ClusterWritable>(){}.getType();
    // internal cluster ID
    public int clusterid;
    // list of nodes contained, the last url is a possible candidate url 
    // i.e. it was added last to the cluster and may therefore be emitted
    // if it immediately qualifies.
    public ArrayList<NodeWritable> nodes = new ArrayList();

    public ClusterWritable() {
    }
    
    @Override
    public void read(ClusterFile f) {
        this.clusterid = f.clusterid.get();
        this.nodes = f.urls.get();
    }

    @Override
    public void write(ClusterFile file) {
        file.clusterid.set(clusterid);
        file.urls.set(nodes);
        file.write();
    }
    
    /**
     * @return the candidate Node, i.e. the last node that was added to the cluster
     */
    public NodeWritable getCandidateNode() {
        return nodes.get((nodes.size()-1));
    }
    
    @Override
    protected Type getType() {
        return type;
    }

    @Override
    protected void getAttributes(Object o) {
        if (o instanceof ClusterWritable) {
            ClusterWritable u = (ClusterWritable) o;
            this.clusterid = u.clusterid;
            this.nodes = u.nodes;
        }
    }
    
}
