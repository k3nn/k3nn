package scrape;

import io.github.repir.tools.ByteSearch.ByteSearch;
import io.github.repir.tools.ByteSearch.ByteSearchSection;
import io.github.repir.tools.ByteSearch.ByteSection;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.Lib.WebTools;
import io.github.repir.tools.Lib.WebTools.UrlResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import scrape1.job.Domain_IA;

/**
 *
 * @author jeroen
 */
public class Scrape {

    public static final Log log = new Log(Scrape.class);
    public TreeMap<String, String> times = new TreeMap();

    public Scrape(String url) {
        try {
            byte[] content = getPage(url).content;
            getDates(content);
        } catch (Exception ex) {
            log.exception(ex, "Scrape %s", url);
        }
    }

   public UrlResult getPage(String url) throws Exception {
       WebTools.UrlResult content = WebTools.getUrlByteArray(url, 2000);
       return content;
   }

    ByteSection link = ByteSection.create("<a ", ">").innerQuoteSafe();
    ByteSearch yearlabel = ByteSearch.create("class=\"year\\-label\"");
    ByteSection snapshot = ByteSection.create("href=\"", "\"");
    ByteSearch replaceyear = ByteSearch.create("0000000\\*");
    ByteSection timesnapshot = ByteSection.create("/web/", "/");

    public void getDates(byte[] content) {
        ArrayList<ByteSearchSection> findAllSections = link.findAllSections(content);
        for (ByteSearchSection section : findAllSections) {
            String hyperlink = snapshot.extract(section);
            String time = timesnapshot.extract(section);
            if (time != null) {
                time = replaceyear.replace(time, "000000*");
                hyperlink = replaceyear.replace(hyperlink, "000000*");
                log.info("%s %s", time, hyperlink);
                times.put(time, hyperlink);
            }
        }
    }

    public String getYearTime(String year) {
        for (Map.Entry<String, String> entry : times.entrySet()) {
            if (entry.getKey().startsWith(year)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public String getYearUrl(String year) {
        for (Map.Entry<String, String> entry : times.entrySet()) {
            if (entry.getKey().startsWith(year)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static void main(String[] args) {
        Scrape s = new Scrape("https://web.archive.org/web/*/nytimes.com");
        String year = s.getYearUrl("2012");
        log.info("year %s", year);
        Scrape y = new Scrape("https://web.archive.org" + year);
        String first = y.getYearUrl("2012");
        log.info("year %s", first);
        ScrapeMain day = new ScrapeMain(first);
        
        int domain = Domain_IA.instance.getDomainForHost("nytimes.com");
        ByteSearch articlefilter = Domain_IA.instance.getRegex(domain);
        for (String link : day.getArticles(articlefilter)) {
            log.printf("%s", link);
        }
    }
}
