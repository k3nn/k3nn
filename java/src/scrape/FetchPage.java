package scrape;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchSection;
import io.github.repir.tools.search.ByteSection;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.WebTools;
import io.github.repir.tools.lib.WebTools.UrlResult;

/**
 *
 * @author jeroen
 */
public class FetchPage {

    public static final Log log = new Log(FetchPage.class);
    static ByteSearch redirection = ByteSearch.create("<p class=\"code shift red\">Got an HTTP 302 response at crawl time</p>"
            + "\\s*<p class=\"code\">Redirecting to\\.+</p>");
    static ByteSection redirect = ByteSection.create("<p class=\"impatient\"><a href=\"", "\"");
    static ByteSearch notfound = ByteSearch.create("<title>Internet Archive Wayback Machine</title>");

    public static UrlResult fetchPage(String url) {
        UrlResult resultpage = null;
        for (int t = 0; t < 4 && resultpage == null; t++) {
            try {
                String urlattempt = url;
                if (t % 2 == 1) {
                    int indexOf = url.indexOf("?");
                    if (indexOf > 0)
                        urlattempt = url.substring(0, indexOf);
                }
                resultpage = getPage("https://web.archive.org" + urlattempt);
                if (notfound.exists(resultpage.content)) {
                    resultpage = null;
                }
            } catch (Exception ex) {
                resultpage = null;
                log.exception(ex, "Scrape %s", url);
                log.sleep(1000);
            }
        }
        return resultpage;
    }
    
    public static UrlResult getPage(String url) throws ScrapeMain.IllegalPageException {
        while (true) {
            UrlResult content = WebTools.getUrlByteArray(url, 20000);
            if (content != null) {
                log.info("%s %b", url, redirection.exists(content.content));
                //log.info("%d %s", content.responsecode, new String(content.content));
                if (redirection.exists(content.content)) {
                    ByteSearchSection findPos = redirect.findPos(content.content);
                    log.info("redirect %b %s", findPos.found(), findPos.toString());
                    if (findPos.found()) {
                        String url2 = findPos.toString();
                        if (url.equals(url2)) {
                            url = url2;
                            continue;
                        }
                    }
                } else {
                    return content;
                }
            }
            ScrapeMain.IllegalPageException ex = new ScrapeMain.IllegalPageException("Illegal page for " + url);
            throw ex;
        }
    }
    
}
