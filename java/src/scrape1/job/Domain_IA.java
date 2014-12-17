package scrape1.job;

import kba1raw.Domain_KBA;

/**
 *
 * @author jeroen
 */
public class Domain_IA extends Domain_KBA {
   public static Domain_IA instance = new Domain_IA();
   
   protected Domain_IA() {
       super("newssites_ia.txt");
   }
}
