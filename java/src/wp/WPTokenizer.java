package wp;

import io.github.repir.tools.search.ByteSearchSection;
import io.github.repir.tools.extract.Content;
import io.github.repir.tools.extract.ExtractChannel;
import io.github.repir.tools.extract.ExtractorConf;
import io.github.repir.tools.extract.modules.ConvertHtmlASCIICodes;
import io.github.repir.tools.extract.modules.ConvertHtmlAmpersand;
import io.github.repir.tools.extract.modules.ConvertHtmlSpecialCodes;
import io.github.repir.tools.extract.modules.ConvertUnicodeDiacritics;
import io.github.repir.tools.extract.modules.MarkText;
import io.github.repir.tools.extract.modules.MarkTitle;
import io.github.repir.tools.extract.modules.MarkWikipediaLink;
import io.github.repir.tools.extract.modules.MarkWikipediaMLMacro;
import io.github.repir.tools.extract.modules.RemoveRedirect;
import io.github.repir.tools.extract.modules.RemoveTemplate;
import io.github.repir.tools.extract.modules.RemoveWikipediaMacros;
import io.github.repir.tools.extract.modules.TokenWord;
import io.github.repir.tools.extract.modules.TokenizerRegexConf;
import io.github.repir.tools.lib.Log;
import java.util.ArrayList;

/**
 *
 * @author jeroen
 */
public class WPTokenizer extends ExtractorConf {

    public static final Log log = new Log(WPTokenizer.class);
    TokenizerRegexConf tokenizer = createTokenizer();
    Content result;
    
    public WPTokenizer() {
        super();
        this.addPreProcessor(RemoveRedirect.class);
        this.addPreProcessor(RemoveTemplate.class);
        this.addPreProcessor(ConvertHtmlASCIICodes.class);
        this.addPreProcessor(ConvertHtmlSpecialCodes.class);
        this.addPreProcessor(ConvertUnicodeDiacritics.class);

        //this.addSectionMarker(MarkWikipediaTable.class, "all", "table");
        //this.addSectionMarker(MarkTitle.class, "all", "title");
        this.addSectionMarker(MarkText.class, "all", "text");
        this.addSectionMarker(MarkWikipediaLink.class, "text", "link");

        this.addSectionProcess("link", "clean", "link");

        this.addProcess("clean", RemoveWikipediaMacros.class);
        this.addProcess("clean", ConvertHtmlAmpersand.class);
    }

    private TokenizerRegexConf createTokenizer() {
        TokenizerRegexConf tokenizerRegex = new TokenizerRegexConf(this, "tokenize");
        tokenizerRegex.setupTokenProcessor("word", TokenWord.class);
        return tokenizerRegex;
    }

    public Content tokenize(byte content[]) {
        result = process(content);
        return result;
    }
    
    public Content tokenize(String text) {
        return tokenize(text.getBytes());
    }
    
    public ExtractChannel getTitle() {
        return result.get("tokenizedtitle");
    }

    public ArrayList<byte[]> getTables() {
        ArrayList<byte[]> tables = new ArrayList();
        for (ByteSearchSection section : result.getSectionPos("table")) {
            byte[] table = new byte[section.end - section.start];
            System.arraycopy(result.content, section.start, table, 0, table.length);
            tables.add(table);
        }
        return tables;
    }

    public ArrayList<byte[]> getMacros() {
        ArrayList<byte[]> macros = new ArrayList();
        for (ByteSearchSection section : result.getSectionPos("macro")) {
            byte[] macro = new byte[section.end - section.start];
            System.arraycopy(result.content, section.start, macro, 0, macro.length);
            macros.add(macro);
        }
        return macros;
    }
    
    public ArrayList<ByteSearchSection> getLinks() {
        log.info("getLinks %d", result.getSectionPos("link").size());
        return result.getSectionPos("link");
    }
}
