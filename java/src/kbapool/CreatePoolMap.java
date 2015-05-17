package kbapool;

import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMapSet;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.ContextTools;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.lib.StrTools;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import kbaeval.PoolWritable;
import kbapool.CreatePoolJob.Item;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author jeroen
 */
public class CreatePoolMap extends Mapper<LongWritable, SentenceWritable, NullWritable, PoolWritable> {

    public static final Log log = new Log(CreatePoolMap.class);
    HashMap<String, Item> map;

    @Override
    public void setup(Context context) throws IOException {
        Conf conf = ContextTools.getConfiguration(context);
        map = CreatePoolJob.get(conf);
    }

    @Override
    public void map(LongWritable key, SentenceWritable value, Context context) throws IOException, InterruptedException {
        if (value.sentenceNumber == 0)
            value.sentenceNumber = -1;
        else if (value.sentenceNumber == -1)
            value.sentenceNumber = 0;
        String docid = value.getDocumentID();
        Item item = map.get(Item.toItemKey(value));
        if (item != null) {
            PoolWritable record = new PoolWritable();
            record.update_id = docid + "-" + value.sentenceNumber;
            record.query_id = item.querynr;
            record.doc_id = value.getDocumentID();
            record.sentence_id = value.sentenceNumber;
            record.update_len = StrTools.countIndexOf(value.content, ' ') + 2;
            record.update_text = value.content.replaceAll("\"", "").replaceAll("\\s\\s+", " ");
            context.write(NullWritable.get(), record);
        }
    }
}
