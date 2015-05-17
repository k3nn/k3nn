package Cluster;

import java.util.ArrayList;
import java.util.UUID;
/**
 *
 * @author jeroen
 */
public class NodeWritable {
    // internal sentence ID, which is also used as node ID    
    public long sentenceID;
    // publication/crawl time of the original document 
    public long creationtime;
    // often a sentence extracted from a document in the collection
    public String content;
    // corresponds to number assigned by Domain_KBA, using a list of domains and the url it was taken
    public int domain;
    // internal node ID's of max K nearest neighbors
    public String nnid;
    // similarity scores of NN
    public String nnscore;
    // collection ID of the original document
    public String docid;
    // sentence nr of the content in the original document. 0 is the title.
    public int sentenceNumber;

    public NodeWritable() {
    }
    
    public ArrayList<Long> getNN() {
        String parts[] = nnid.split(",");
        ArrayList<Long> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Long.parseLong(p));
            }
        }
        return list;
    }

    public ArrayList<Double> getNNScore() {
        String parts[] = nnscore.split(",");
        ArrayList<Double> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Double.parseDouble(p));
            }
        }
        return list;
    }
    
    public UUID getUUID() {
        String uuids = docid.substring(docid.indexOf('-')+1);
        if (uuids.length() != 32) {
            throw new IllegalArgumentException("Invalid UUID string: " + uuids);
        }

        long mostSigBits = Long.valueOf(uuids.substring(0, 8), 16);
        mostSigBits <<= 32;
        mostSigBits |= Long.valueOf(uuids.substring(8, 16), 16);

        long leastSigBits = Long.valueOf(uuids.substring(16, 24), 16);
        leastSigBits <<= 32;
        leastSigBits |= Long.valueOf(uuids.substring(24), 16);

        UUID uuid = new UUID(mostSigBits, leastSigBits);

        return uuid;
    }    
}
