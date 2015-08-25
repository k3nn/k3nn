package kba1SourceToSentences;

import io.github.htools.search.ByteRegex;
import io.github.htools.search.ByteSearchPosition;
import io.github.htools.io.Datafile;
import io.github.htools.io.ResourceDataIn;
import io.github.htools.lib.Log;

/**
 * assigns a domain ID, based on a URL, from a list of domains used
 * @author jeroen
 */
public class NewsDomains {

    public static final Log log = new Log(NewsDomains.class);
    private static final String defaultResourceFile = "newssites_kba.txt";
    public static final NewsDomains instance = new NewsDomains();
    // resource file that contains a list of domains used, with a pseudo regex
    // that is used to determine if a page within a domain is indeed a 
    // news article
    // list of hosts for domainID 
    private String[] host;
    // all regex for the domains combined into one expression, that returns a 
    // pattern number to indicate which domain matched, used as internal domain ID
    ByteRegex domainPattern;

    public NewsDomains(String resourcefile) {
        domainPattern = createDomainPattern(resourcefile);
    }

    private NewsDomains() {
        this(defaultResourceFile);
    }

    private ByteRegex createDomainPattern(String resourcefilename) {
        String c = getDatafile(resourcefilename).readAsString();
        String[] inputlines = c.split("\n");
        host = new String[inputlines.length];

        // construct a regex that identifies if a page within a domain is a
        // news article. In tje resource file, %W, %A, %N, %Y are used to
        // express different wildcard matches, see below. In the resource
        // file, a . is a literal period and a dash a literal dash.
        ByteRegex[] regex = new ByteRegex[inputlines.length];
        for (int i = 0; i < inputlines.length; i++) {
            String p = inputlines[i];
            host[i] = p.substring(0, p.indexOf('/'));
            p = p.replace("-", "\\-"); // convert dash to literal dash
            p = p.replace(".", "\\."); // convert . to a literal .
            p = p.replace("%W", "[^\\?]*?"); // any character except a ?
            p = p.replace("%A", ".*?"); // any character
            p = p.replace("%N", "[^/\\?]*"); // any text except a / or a ?
            p = p.replace("%Y", "201\\d"); // hack yo detect a year, only works for years 201x
            if (p.length() > 0) {
                regex[i] = new ByteRegex(p);
            }
        }
        return ByteRegex.combine(regex);
    }

    private Datafile getDatafile(String resourcefilename) {
        return new Datafile(new ResourceDataIn(NewsDomains.class, "resources/" + resourcefilename));
    }

    private String[] getDomains() {
        return host;
    }

    /**
     * @param domainID
     * @return host String for given internal domain ID
     */
    public String getHost(int domainID) {
        return host[domainID];
    }

    /**
     * @param url
     * @return internal domain number for given URL, or -1 if domain is unknown
     */
    public int getDomainForUrl(String url) {
        ByteSearchPosition findPos = domainPattern.findPos(url);
        return findPos.found() ? findPos.pattern : -1;
    }
    
    public static void main(String[] args) {
        String[] domains = new NewsDomains().getDomains();
        for (int i = 0; i < domains.length; i++) {
            log.printf("%d %s", i, domains[i]);
        }
    }
}
