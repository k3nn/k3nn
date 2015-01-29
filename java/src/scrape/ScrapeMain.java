package scrape;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchPosition;
import io.github.repir.tools.search.ByteSearchSection;
import io.github.repir.tools.search.ByteSection;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.WebTools;
import io.github.repir.tools.lib.WebTools.UrlResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import scrape1domain.Domain_IA;

/**
 *
 * @author jeroen
 */
public class ScrapeMain {

    public static final Log log = new Log(ScrapeMain.class);
    UrlResult resultpage;
    String url;
    ByteSection link = ByteSection.create("<a ", ">").innerQuoteSafe();
    ByteSearch redirection = ByteSearch.create("<p class=\"code shift red\">Got an HTTP 302 response at crawl time</p>"
            + "\\s*<p class=\"code\">Redirecting to\\.+</p>");
    ByteSection redirect = ByteSection.create("<p class=\"impatient\"><a href=\"", "\"");
    ByteSearch domain = ByteSearch.create(".*?://[^\\./]+\\.[^/]+/");
    ByteSection href;

    public ScrapeMain(String url) {
        this.url = url;
        for (int t = 0; t < 4 && resultpage == null; t++) {
            try {
                resultpage = getPage("https://web.archive.org" + url);
                if (resultpage != null) {
                    String hrefstring = ByteSearch.escape(resultpage.redirected.getPath().toString());
                    href = getDomainPattern(hrefstring);
                    log.info("href %s", href);
                }
            } catch (Exception ex) {
                resultpage = null;
                log.exception(ex, "Scrape %s", url);
                log.sleep(5000);
            }
        }
    }
    
    public ByteSection getDomainPattern(String actualurl) {
        ByteSearchPosition find = domain.findPos(actualurl);
        if (find.found()) {
            return new ByteSection(find.toString(), "(?=\"|')");
        }
        return null;
    }

    public UrlResult getResult() {
        return resultpage;
    }

    public ArrayList<String> getArticles(ByteSearch articlepattern) {
        ArrayList<String> links = new ArrayList();
        if (resultpage != null && href != null) {
            ArrayList<ByteSearchSection> all = link.findAllSections(resultpage.content);
            for (ByteSearchSection section : all) {
                String href1 = href.extractOuterTrim(section);
                if (href1 != null) {
                    log.info("%b %b %s %s", href.exists(section), articlepattern.exists(href1), section.toOuterString(), href1);
                    //href1 = href1.substring(0, href1.length() - 1);
                    if (articlepattern.exists(href1)) {
                        links.add(href1);
                    }
                }
            }
        }
        log.info("size %d", links.size());
        return links;
    }

    public ArrayList<String> getNonArticles(ByteSearch articlepattern) {
        ArrayList<String> links = new ArrayList();
        if (resultpage != null && href != null) {
            ArrayList<ByteSearchSection> all = link.findAllSections(resultpage.content);
            for (ByteSearchSection section : all) {
                String href1 = href.extractOuterTrim(section);
                log.info("%s %s %b %s", articlepattern.toString(), href1, href.exists(section), section.toOuterString());
                if (href1 != null) {
                    log.info("%b", !articlepattern.exists(section));
                    //href1 = href1.substring(0, href1.length() - 1);
                    if (!articlepattern.exists(section)) {
                        links.add(href1);
                    }
                }
            }
        }
        return links;
    }

    public UrlResult getPage(String url) throws IllegalPageException {
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
            IllegalPageException ex = new IllegalPageException("Illegal page for " + url);
            throw ex;
        }
    }

    public static class IllegalPageException extends IOException {

        public IllegalPageException(String message) {
            super(message);
        }
    }

    public static void main(String[] args) {
        ScrapeMain day = new ScrapeMain("/web/20120101003916/http://nytimes.com/");
        int domain = Domain_IA.instance.getDomainForHost("nytimes.com");
        ByteSearch articlefilter = Domain_IA.instance.getRegex(domain);
        for (String l : day.getArticles(articlefilter)) {
            log.printf("%s", l);
        }
    }
}
