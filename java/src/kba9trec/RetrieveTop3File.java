package kba9trec;

import KNN.Url;
import KNN.UrlD;
import RelCluster.RelClusterFile;
import RelCluster.RelClusterWritable;
import StreamCluster.StreamClusterFile;
import StreamCluster.StreamClusterWritable;
import io.github.repir.tools.extract.modules.RemoveStopwordsInquery;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.extract.modules.RemoveStopwordsSmart;
import io.github.repir.tools.lib.Log;
import java.io.IOException;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import kbaeval.TopicWritable;

public class RetrieveTop3File extends RetrieveTop3 {

    private static final Log log = new Log(RetrieveTop3File.class);
    TrecFile outfile;
    TrecWritable record = new TrecWritable();
    RelClusterWritable recordcluster = new RelClusterWritable();
    RelClusterFile outclusterfile;

    public RetrieveTop3File(Datafile out) {
        outfile = new TrecFile(out);
        outfile.openWrite();
        outclusterfile = new RelClusterFile(out.getDir().getFile(out.getFilename() + ".titles"));
        outclusterfile.openWrite();
        deftokenizer.addEndPipeline(RemoveStopwordsInquery.class);
        //deftokenizer.addEndPipeline(StemTokens.class);
    }

    public void process(TopicWritable topic, Datafile in) throws IOException, InterruptedException {
        init(topic.id, topic.start, topic.end, topic.query);
        StreamClusterFile cf = new StreamClusterFile(in);
        cf.setBufferSize(100000000);
        for (StreamClusterWritable cw : cf) {
            stream(cw);
            if (watch) {
                close();
                log.exit();
            }
        }
    }

    @Override
    public void out(int topic, Url u, String title) {
        record.document = ((UrlD) u).getDocumentID();
        record.timestamp = u.getCreationTime();
        record.topic = topic;
        record.sentence = ((UrlD) u).sentence;
        record.write(outfile);
        recordcluster.clusterid = topic;
        recordcluster.creationtime = u.getCreationTime();
        recordcluster.domain = u.getDomain();
        recordcluster.nnid = u.getNN();
        recordcluster.nnscore = u.getScore();
        recordcluster.title = title;
        recordcluster.urlid = u.getID();
        recordcluster.documentid = ((UrlD) u).getDocumentID();
        recordcluster.row = ((UrlD) u).sentence;
        recordcluster.write(outclusterfile);
    }

    public void close() {
        outfile.closeWrite();
        outclusterfile.closeWrite();
    }
}
