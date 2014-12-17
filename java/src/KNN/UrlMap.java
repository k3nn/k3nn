package KNN;

import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.UrlStrTools;
import java.util.HashMap;
/**
 *
 * @author jeroen
 */
public class UrlMap extends HashMap<Integer, UrlM> {
   public static final Log log = new Log( UrlMap.class );
    HashMap<String, Integer> domains = new HashMap();

    
    public UrlM put(int urlid, UrlM url) {
        return super.put(urlid, url);
    }
    
    public int unclustered() {
        int count = 0;
        for (UrlM u : values()) {
            if (!u.isClustered()) {
                count++;
            }
        }
        return count;
    }
    
    public int getDomain(String url) {
        String host = UrlStrTools.host(url);
        Integer domain = domains.get(host);
        if (domain == null) {
            domain = domains.size();
            domains.put(host, domain);
        }
        return domain;
    }


}
