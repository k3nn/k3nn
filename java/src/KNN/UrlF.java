package KNN;

import io.github.repir.tools.Lib.Log;
import static io.github.repir.tools.Lib.PrintTools.sprintf;
import java.util.Collection;

/**
 *
 * @author jeroen
 */
public class UrlF extends UrlM {

    static Log log = new Log(UrlF.class);

    public UrlF(int id, int domain, long creationtime, Collection<String> features) {
        super(id, domain, creationtime, features);
    }
}
