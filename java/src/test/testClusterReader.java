package test;

import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.Lib.Log;
import streamcorpus.sentence.SentenceFile;
import streamcorpus.sentence.SentenceWritable;
/**
 *
 * @author jeroen
 */
public class testClusterReader {
   public static final Log log = new Log( testClusterReader.class );

    public static void main(String[] args) {
        Datafile df = new Datafile(args[0]);
        SentenceFile cf = new SentenceFile(df);
        int lastid = -1;
        for (SentenceWritable cw : cf) {
            log.info("%d %d", cw.id, df.getOffset());
            if (cw.id == 142903426) {
                log.info("%d %s", cw.id, cw.sentence);
                break;
            }
        }
    }
}
