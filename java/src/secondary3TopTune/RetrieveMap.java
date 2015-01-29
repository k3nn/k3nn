package secondary3TopTune;

import KNN.Url;
import KNN.UrlD;
import RelCluster.RelClusterWritable;
import StreamCluster.StreamClusterWritable;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import kba9trec.RetrieveTop3;
import kbaeval.TrecWritable;
import org.apache.hadoop.mapreduce.Mapper;
import secondary3TopTune.RetrieveJob.Setting;

/**
 *
 * @author jeroen
 */
public class RetrieveMap extends Mapper<Setting, StreamClusterWritable, Setting, RelClusterWritable> {

    public static final Log log = new Log(RetrieveMap.class);
    Context context;
    RelClusterWritable record = new RelClusterWritable();
    Setting key;
    Retriever retriever = null;

    @Override
    public void setup(Context context) throws IOException {
        this.context = context;
    }

    @Override
    public void map(Setting key, StreamClusterWritable value, Context context) throws IOException, InterruptedException {
        if (retriever == null) {
            this.key = key;
            retriever = new Retriever(key);
        }
        retriever.stream(value);
    }

    @Override
    public void cleanup(Context context) {
        retriever = null;
    }

    class Retriever extends RetrieveTop3 {

        Retriever(Setting setting) throws IOException {
            this.RelevanceModelHours = setting.hours;
            this.maxterms = setting.length;
            this.mingainratio = setting.gainratio;
            this.topk = setting.topk;
            this.init(setting.topicid, setting.topicstart, setting.topicend, setting.query);
            log.info("topic %d %d %d %s", setting.topicid, setting.topicstart, setting.topicend, setting.query);
            log.info("settings %f %f %d %d", setting.gainratio, setting.hours, setting.length, setting.topk);
        }

        @Override
        public void out(int topic, Url u, String title) throws IOException, InterruptedException {
            record.clusterid = topic;
            record.creationtime = u.getCreationTime();
            record.domain = u.getDomain();
            record.nnid = u.getNN();
            record.nnscore = u.getScore();
            record.title = title;
            record.urlid = u.getID();
            record.documentid = ((UrlD) u).getDocumentID();
            record.row = ((UrlD) u).sentence;
            log.info("out %f %f %d %d %d %d %s", key.gainratio, key.hours, key.length, key.topk, key.topicid,
                    record.creationtime, record.documentid);
            context.write(key, record);
        }
    }
}
