package streamcorpus;

import io.github.repir.tools.search.ByteRegex;
import io.github.repir.tools.lib.Log;
import scrape1domain.Domain_IA;
/**
 *
 * @author jeroen
 */
public class testIA {
   public static final Log log = new Log( testIA.class );

    public static void main(String[] args) {
        Domain_IA domain = Domain_IA.instance;
        int d = domain.getDomainForHost("www.jpost.com");
        ByteRegex regex = domain.getRegex(d);
        String url = "/web/20130213103257/http://www.jpost.com/Sci-Tech/Article.aspx?id=303090";
        log.info("%s %b", regex.toString(), regex.exists(url));
    }
}
