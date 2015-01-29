package kba1raw_jpost;

import kba1raw_sentences.*;
import kba1raw.*;
import io.github.repir.tools.search.ByteRegex;
import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchPosition;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.io.ResourceDataIn;
import io.github.repir.tools.lib.Log;
import java.util.ArrayList;

/**
 *
 * @author jeroen
 */
public class Domain_JPost {

    public static final Log log = new Log(Domain_JPost.class);
    public static final Domain_JPost instance = new Domain_JPost();
    private String[] regexstring;
    private String[] host;
    private ByteRegex[] regex;
    public String resourcefile = "newssites_kba_jpost.txt";
    ByteRegex filter;

    protected Domain_JPost(String resourcefile) {
        this.resourcefile = resourcefile;
        filter = createFilter();
    }

    protected Domain_JPost() {
        this("newssites_kba_jpost.txt");
    }

    private ByteRegex createFilter() {
        String c = getDatafile().readAsString();
        regexstring = c.split("\n");
        host = new String[regexstring.length];

        regex = new ByteRegex[regexstring.length];
        for (int i = 0; i < regexstring.length; i++) {
            String p = regexstring[i];
            host[i] = p.substring(0, p.indexOf('/'));
            p = p.replace(".", "\\.");
            p = p.replace("%W", ".*?");
            p = p.replace("%N", "[^/]*");
            p = p.replace("%Y", "201\\d");
            //log.info("%s", p);
            if (p.length() > 0) {
                regex[i] = new ByteRegex(p);
            }
        }
        return ByteRegex.combine(regex);
    }

    private Datafile getDatafile() {
        return new Datafile(new ResourceDataIn(Domain_JPost.class, "resources/" + resourcefile));
    }

    public String[] getDomains() {
        return host;
    }

    public int getDomainForHost(String h) {
        for (int i = 0; i < this.host.length; i++) {
            if (this.host[i].contains(h)) {
                return i;
            }
        }
        return -1;
    }

    public int getDomainForRegex(String regex) {
        for (int i = 0; i < regexstring.length; i++) {
            if (this.regexstring[i].contains(regex)) {
                return i;
            }
        }
        return -1;
    }

    public String getPatternString(int i) {
        return regexstring[i];
    }

    public String getHost(int i) {
        return host[i];
    }

    public ByteRegex getRegex(int pattern) {
        return regex[pattern];
    }

    public int getDomainForUrl(String url) {
        ByteSearchPosition findPos = filter.findPos(url);
        return findPos.found() ? findPos.pattern : -1;
    }
    
    public static void main(String[] args) {
        String[] domains = new Domain_JPost().getDomains();
        for (int i = 0; i < domains.length; i++) {
            log.printf("%d %s", i, domains[i]);
        }
    }
}
