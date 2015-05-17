package test;

import io.github.repir.tools.lib.Log;
import java.util.UUID;
/**
 *
 * @author jeroen
 */
public class convertUUID {
   public static final Log log = new Log( convertUUID.class );

    public static void main(String[] args) {
        UUID uuid = UUID.fromString(args[0]);
        log.printf("%d %d", uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
   
}
