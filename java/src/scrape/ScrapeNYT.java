package scrape;

import io.github.repir.tools.ByteSearch.ByteSearch;
import io.github.repir.tools.Lib.Log;
import java.util.HashSet;
import scrape1.job.Domain_IA;

/**
 *
 * @author jeroen
 */
public class ScrapeNYT {
   public static final Log log = new Log( ScrapeNYT.class );
   HashSet<String> articles = new HashSet();
   HashSet<String> subpages = new HashSet();
   ByteSearch articlematcher;
   
   public ScrapeNYT(String page, String articleregex) {
       articlematcher = ByteSearch.create(articleregex);
       
       ScrapeMain day = new ScrapeMain(page);
       int domain = Domain_IA.instance.getDomainForHost("nytimes.com");
       ByteSearch articlefilter = Domain_IA.instance.getRegex(domain);
       for (String link : day.getArticles(articlefilter)) {
            if (articlematcher.exists(link)) {
                articles.add(link);
            } else {
                subpages.add(link);
            }
       }
    }
   
    public static void main(String[] args) {
        new ScrapeNYT("/web/20120101003916/http://nytimes.com/", "\\d\\d\\d\\d/");
    }
}
