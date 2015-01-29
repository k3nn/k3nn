package KNN;

import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public interface UrlClusterListener {

    public void urlChanged(Url url, HashSet<Url> urls);

}
