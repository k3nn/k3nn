package KNN;

import io.github.repir.tools.lib.Log;

/**
 *
 * @author jeroen
 */
public class UrlB extends Url {

    static Log log = new Log(UrlB.class);
    
    public UrlB(int id, long creationtime) {
        super(id);
        this.setCreationTime(creationtime);
    }
}
