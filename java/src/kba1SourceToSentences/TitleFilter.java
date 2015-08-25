package kba1SourceToSentences;

import io.github.htools.io.web.UrlStrTools;

/**
 * Cleans non content parts from titles extracted from HTML Pages of news
 * sites. In general, a dash or pipe symbol that is more than half way past the
 * title's length indicates non-content. To remove site name, domain specific
 * rules were created.
 * <p/>
 * Note, removal of non-content is not perfect.
 * @author jeroen
 */
public class TitleFilter {
    public static String filter(String url, String title) {
        String host = UrlStrTools.host(UrlStrTools.stripMethod(url));
        return filterHost(host, title);
    }
    
    public static String filterHost(String host, String title) {
        title = title.trim();
        while (title.lastIndexOf(" – ") > title.length() / 2 && !host.equals("earthquake.usgs.gov"))
            title = stripfromlast(title, " – ");
        while (title.lastIndexOf(" - ") > title.length() / 2 && !host.equals("earthquake.usgs.gov"))
            title = stripfromlast(title, " - ");
        while (title.lastIndexOf(" | ") > title.length() / 2)
            title = stripfromlast(title, " | ");
        if (host.equals("www.nytimes.com")) {
            title = strip(title, "NYTimes com");
        }
        if (host.equals("www.foxnews.com")) {
            title = strip(title, "Fox News");
        }
        if (host.equals("www.usnews.com")) {
            title = strip(title, "US News");
        }
        if (host.equals("abcnews.go.com")) {
            title = stripfrom(title, "ABC News");
        }
        if (host.equals("www.abc.net.au")) {
            title = stripfrom(title, " - ABC ");
        }
        if (host.equals("www.washingtonpost.com")) {
            title = strip(title, "The Washington Post");
        }
        if (host.equals("www.washingtontimes.com")) {
            title = strip(title, "Washington Times");
        }
        if (host.equals("www.bbc.co.uk")) {
            title = stripstart(title, "BBC News");
        }
        if (host.equals("www.bbc.co.uk")) {
            title = stripstart(title, "BBC Sport");
        }
        if (host.equals("www.bbc.co.uk")) {
            title = stripfrom(title, " BBC");
        }
        if (host.equals("www.cbc.ca")) {
            title = stripfrom(title, " CBC ");
        }
        if (host.equals("www.dailymail.co.uk")) {
            title = strip(title, "Mail Online");
        }
        if (host.equals("www.nydailymews.com")) {
            title = strip(title, "NY Daily News");
        }
        if (host.equals("www.ctvnews.com")) {
            title = strip(title, "CTV News");
        }
        if (host.equals("www.foxbusiness.com")) {
            title = strip(title, "Fox Business");
        }
        if (host.equals("www.businessweek.com")) {
            title = strip(title, "Businessweek");
        }
        if (host.equals("www.cnn.com")) {
            title = strip(title, "CNN com");
        }
        if (host.equals("www.cbsnews.com")) {
            title = stripfrom(title, "CBS News");
        }
        if (host.equals("www.latimes.com")) {
            title = strip(title, " story");
        }
        if (host.equals("www.latimes.com")) {
            title = strip(title, "LA Times");
        }
        if (host.equals("www.latimes.com")) {
            title = strip(title, "latimes.com");
        }
        if (host.equals("www.latimes.com")) {
            title = stripstart(title, "la et ms");
        }
        if (host.equals("timesofindia.indiatimes.com")) {
            title = strip(title, "The Times of India");
        }
        if (host.equals("www.reuters.com")) {
            title = strip(title, "Reuters");
        }
        if (host.equals("www.forbes.com")) {
            title = strip(title, "Forbes");
        }
        if (host.equals("www.itv.com")) {
            title = strip(title, "ITV News");
        }
        if (host.equals("www.ft.com")) {
            title = strip(title, "FT com");
        }
        if (host.equals("au.news.yahoo.com")) {
            title = stripfrom(title, "Yahoo");
        }
        if (host.equals("www.aol.com")) {
            title = strip(title, "AOL com");
        }
        if (host.equals("www.aol.com")) {
            title = stripstart(title, "AOL com");
        }
        if (host.equals("www.businessinsider.com")) {
            title = strip(title, "Business Insider");
        }
        if (host.equals("www.kron4.com")) {
            title = stripfrom(title, "KRON4");
        }
        if (host.equals("online.wsj.com")) {
            title = strip(title, "WSJ");
        }
        if (host.equals("www.independent.co.uk")) {
            title = strip(title, "The Independent");
        }
        if (host.equals("time.com")) {
            title = strip(title, "TIME");
        }
        if (host.equals("www.rollingstone.com")) {
            title = strip(title, "Rolling Stone");
        }
        if (host.equals("blogs.wsj.com")) {
            title = strip(title, "WSJ");
        }
        if (host.equals("sportsillustrated.cnn.com")) {
            title = strip(title, "SI com");
        }
        if (host.equals("www.theglobeandmail.com")) {
            title = strip(title, "The Globe and Mail");
        }
        if (host.equals("www.mercurynews.com")) {
            title = strip(title, "Mercury News");
        }
        if (host.equals("www.dw.de")) {
            title = stripfrom(title, "DW DE");
        }
        if (host.equals("www.jpost.com")) {
            title = stripfrom(title, "JPost");
        }
        if (host.equals("www.theguardian.com")) {
            title = strip(title, " theguardian.com");
        }
        if (host.equals("www.theguardian.com")) {
            title = strip(title, "Comment is free");
        }
        if (host.equals("www.nbcbayarea.com")) {
            title = stripfrom(title, "NBC");
        }
        if (host.endsWith(".nbcnews.com")) {
            title = stripfrom(title, " NBC News");
        }
        if (host.endsWith("usnews.nbcnews.com")) {
            title = stripfrom(title, " U.S. News");
        }
        if (host.equals("www.mirror.co.uk")) {
            title = strip(title, "Mirror Online");
        }
        if (host.equals("www.sfgate.com")) {
            title = strip(title, "SFGate");
        }
        if (host.equals("www.xinhuanet.com")) {
            title = stripfrom(title, "Xinhua");
        }
        if (host.equals("www.chicagotribune.com")) {
            title = strip(title, "chicagotribune.com");
        }
        if (host.equals("www.chicagotribune.com")) {
            title = strip(title, "Chicago Tribune");
        }
        if (host.equals("news.nationalpost.com")) {
            title = strip(title, "National Post");
        }
        if (host.equals("www.npr.org")) {
            title = strip(title, "NPR");
        }
        if (host.equals("www.variety.com")) {
            title = strip(title, "Variety");
        }
        if (host.equals("www.bloomberg.com")) {
            title = strip(title, "Bloomberg");
        }
        if (host.equals("www.startribune.com")) {
            title = strip(title, "Star Tribune");
        }
        if (host.equals("msn.foxsports.com")) {
            title = strip(title, "FOX");
        }
        if (host.equals("hosted2.ap.com")) {
            title = strip(title, "AP News");
        }
        if (host.equals("hosted2.ap.com")) {
            title = stripstart(title, "AP News");
        }
        if (host.equals("www.ktvu.com")) {
            title = strip(title, "ktvu com");
        }
        if (host.equals("rt.com")) {
            title = strip(title, "RT News");
        }
        if (host.equals("www.couriermail.com")) {
            title = strip(title, "The Courier Mail");
        }
        if (host.equals("www.buffalonews.com")) {
            title = stripfrom(title, "The Buffalo News");
        }
        if (host.equals("www.milenio.com")) {
            title = stripfrom(title, "Grupo Milenio");
        }
        if (host.equals("www.hawaiinewsnow.com")) {
            title = stripfrom(title, "Hawaii News Now");
        }
        if (host.equals("voiceofrussia.com")) {
            title = stripfrom(title, "Voice of Russia");
        }
        if (host.equals("www.nature.com")) {
            title = stripfrom(title, " Nature ");
        }
        if (host.equals("www.philstar.com")) {
            title = stripfrom(title, "The Philippine Star");
        }
        if (host.equals("www.philstar.com")) {
            title = strip(title, "philstar com");
        }
        if (host.equals("www.globalpost.com")) {
            title = strip(title, "GlobalPost");
        }
        if (host.equals("bigstory.ap.com")) {
            title = strip(title, "The Big Story");
        }
        if (host.equals("www.jakartapost.com")) {
            title = strip(title, "The Jakarta Post");
        }
        if (host.equals("www.jakartaglobe.com")) {
            title = strip(title, "The Jakarta Globe");
        }
        if (host.equals("www.hindustantimes.com")) {
            title = strip(title, "Hindustan Times");
        }
        if (host.equals("www.themoscowtimes.com")) {
            title = strip(title, "the Moscow Times");
        }
        if (host.equals("www.thenews.com")) {
            title = stripstart(title, "Todays News");
        }
        if (host.equals("www.usnews.com")) {
            title = strip(title, "US News");
        }
        if (host.equals("www.bostonglobe.com")) {
            title = strip(title, "The Boston Globe");
        }
        if (host.equals("www.chron.com")) {
            title = strip(title, "Houston Chronicle");
        }
        if (host.equals("www.gmanetwork.com")) {
            title = strip(title, "GMA News Online");
        }
        if (host.equals("www.thehindu.com")) {
            title = strip(title, "The Hindu");
        }
        if (host.equals("nypost.com")) {
            title = strip(title, "New York Post");
        }
        if (host.equals("www.nasdaq.com")) {
            title = strip(title, "NASDAQ com");
        }
        if (host.equals("www.telegraph.co.uk")) {
            title = strip(title, "Telegraph");
        }
        if (host.equals("www.independent.ie")) {
            title = strip(title, "Independent ie");
        }
        if (host.equals("www.express.co.uk")) {
            title = strip(title, "Daily Express");
        }
        if (host.equals("www.marketwatch.com")) {
            title = strip(title, "WSJ");
        }
        if (host.equals("rte.ie")) {
            title = stripfrom(title, " RT");
        }
        if (host.equals("www.ynetnews.com")) {
            title = strip(title, "Ynetnews");
        }
        if (host.equals("www.thewrap.com")) {
            title = strip(title, "TheWrap");
        }
        if (host.equals("www.thenews.com.pk")) {
            title = strip(title, "thenews com pk");
        }
        if (host.equals("www.thejournal.ie")) {
            title = strip(title, "TheJournal ie");
        }
        if (host.equals("www.worldbulletin.net")) {
            title = strip(title, "Worldbulletin News");
        }
        if (host.equals("www.wired.co.uk")) {
            title = strip(title, "Wired UK");
        }
        if (host.equals("www.theaustralian.com.au")) {
            title = strip(title, "The Australian");
        }
        if (host.equals("www.theage.com.au")) {
            title = strip(title, "theage com au");
        }
        if (host.equals("www.thanhniennews.com")) {
            title = strip(title, "Than Nien Daily");
        }
        if (host.equals("www.nzherald.co.nz")) {
            title = strip(title, "NZ");
        }
        if (host.equals("latino.foxnews.com")) {
            title = strip(title, "Fox News Latino");
        }
        if (host.equals("indianexpress.com")) {
            title = strip(title, "The Indian Express");
        }
        if (host.equals("inblive.in.com")) {
            title = strip(title, "IBNLive");
        }
        if (host.equals("english.ahram.org.eg")) {
            title = strip(title, "Egypt Ahram Online");
        }
        if (host.equals("en.trend.az")) {
            title = strip(title, "Trend Az");
        }
        if (host.equals("en.ria.ru")) {
            title = strip(title, "RIA Novosti");
        }
        if (host.equals("economictimes.indiatimes.com")) {
            title = strip(title, "The Economic Times");
        }
        if (host.equals("www.dn.se")) {
            title = stripstart(title, "NHL com");
        }
        if (host.equals("www.nhl.com")) {
            title = stripfrom(title, "DN SE");
        }
        if (host.equals("www.marketwatch.com")) {
            title = strip(title, "MarketWatch");
        }
        return title.trim();
    }

    public static String strip(String title, String end) {
        return (title.endsWith(end)) ? title.substring(0, title.length() - end.length()) : title;
    }

    public static String stripstart(String title, String start) {
        return (title.startsWith(start)) ? title.substring(start.length()) : title;
    }

    public static String stripfrom(String title, String start) {
        return (title.indexOf(start) > 1) ? title.substring(0, title.indexOf(start)) : title;
    }
    public static String stripfromlast(String title, String start) {
        return (title.lastIndexOf(start) > 1) ? title.substring(0, title.lastIndexOf(start)) : title;
    }
}
