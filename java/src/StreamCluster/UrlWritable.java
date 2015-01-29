package StreamCluster;

import java.util.ArrayList;
import java.util.UUID;
/**
 *
 * @author jeroen
 */
public class UrlWritable {
    public int urlid;
    public long creationtime;
    public String title;
    public int domain;
    public String nnid;
    public String nnscore;
    public String docid;
    public int row;

    public UrlWritable() {
    }
    
    public ArrayList<Integer> getNN() {
        String parts[] = nnid.split(",");
        ArrayList<Integer> list = new ArrayList();
        for (String p : parts) {
            if (p.length() > 0) {
                list.add(Integer.parseInt(p));
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
