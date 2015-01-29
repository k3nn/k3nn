package KNN;

import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.UrlStrTools;
import java.util.HashMap;
/**
 *
 * @author jeroen
 */
public class UrlMap<U extends Url> extends HashMap<Integer, U> {
   public static final Log log = new Log( UrlMap.class );
    HashMap<String, Integer> domains = new HashMap();

    
    public U put(int urlid, U url) {
        return super.put(urlid, url);
    }
    
    public int unclustered() {
        int count = 0;
        for (U u : values()) {
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
