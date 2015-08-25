package test;

import io.github.htools.hadoop.Conf;
import io.github.htools.lib.ArgsParser;
import io.github.htools.lib.Log;
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
