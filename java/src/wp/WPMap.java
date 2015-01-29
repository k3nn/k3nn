package wp;

import io.github.repir.tools.search.ByteSearch;
import io.github.repir.tools.search.ByteSearchPosition;
import io.github.repir.tools.search.ByteSearchSection;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.lib.ByteTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class WPMap extends Mapper<LongWritable, byte[], LinkWritable, IntWritable> {

    public static final Log log = new Log(WPMap.class);
    WPTokenizer tokenizer = new WPTokenizer();
    ByteSearch bar = ByteSearch.create("\\|");
    HashMapInt<LinkWritable> map = new HashMapInt();

    @Override
    public void map(LongWritable key, byte[] document, Context context) throws IOException, InterruptedException {
        tokenizer.tokenize(document);
        ArrayList<ByteSearchSection> links = tokenizer.getLinks();
        for (ByteSearchSection link : links) {
            Tuple2<String, String> anchor = solveLink(link);
            log.info("%s - %s %b %b", anchor.value1, anchor.value2, validEntity(anchor.value1), validAnchor(anchor.value2));
            if (validEntity(anchor.value1) && validAnchor(anchor.value2)) {
               log.info("%s - %s", anchor.value1, anchor.value2);
               LinkWritable w = new LinkWritable();
               w.entity = anchor.value1;
               w.anchortext = anchor.value2;
               map.add(w, 1);
            }
        }
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<LinkWritable, Integer> entry : map.entrySet()) {
            LinkWritable key = entry.getKey();
            key.frequency = entry.getValue();
            IntWritable value = new IntWritable(entry.getValue());
            context.write(key, value);
        }
    }
    
    public boolean validEntity(String entity) {
        int colon = entity.indexOf(':');
        if (colon < 0) {
            colon = entity.indexOf('{');
            if (colon < 0) {
                colon = entity.indexOf('~');
            }
        }
        return colon < 0;
    }
    
    public boolean validAnchor(String anchor) {
        int bar = anchor.indexOf('|');
        if (bar > -1) {
            return false;
        }
        return true;
    }
    
    public Tuple2<String, String> solveLink(ByteSearchSection s) {
        ByteSearchPosition barPos = bar.findPos(s);
        String entity, anchor;
        if (barPos.found()) {
            entity = ByteTools.toFullTrimmedString(s.haystack, s.innerstart, barPos.start);
            s.innerstart = barPos.end;
        } else {
            entity = ByteTools.toFullTrimmedString(s.haystack, s.innerstart, s.innerend);
        }
        anchor = ByteTools.toFullTrimmedString(s.haystack, s.innerstart, s.innerend);
        if (s.end - s.innerend > 2) {
           anchor += ByteTools.toFullTrimmedString(s.haystack, s.innerend + 2, s.end);
        }
        return new Tuple2<String, String>(entity, anchor);
    }
}
