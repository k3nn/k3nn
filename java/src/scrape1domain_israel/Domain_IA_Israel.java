package scrape1domain_israel;

import scrape1domain.*;
import kba1raw.Domain_KBA;

/**
 *
 * @author jeroen
 */
public class Domain_IA_Israel extends Domain_KBA {
   public static Domain_IA_Israel instance = new Domain_IA_Israel();
   
   protected Domain_IA_Israel() {
       super("newssites_ia_israel.txt");
   }
}
