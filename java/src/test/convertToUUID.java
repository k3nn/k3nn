package test;

import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.lib.ArgsParser;
import io.github.repir.tools.lib.Log;
import java.util.UUID;
/**
 *
 * @author jeroen
 */
public class convertToUUID {
   public static final Log log = new Log( convertToUUID.class );

    public static void main(String[] args) {
        Conf ap = new Conf(args, "u1 u2");
        UUID uuid = new UUID(ap.getLong("u1", 0), ap.getLong("u2", 0));
        log.printf("%s", uuid.toString());
    }
   
}
