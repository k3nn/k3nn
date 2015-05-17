package test;

import ClusterNode.ClusterNodeFile;
import ClusterNode.ClusterNodeWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import Sentence.SentenceFile;
import Sentence.SentenceWritable;
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
            log.info("%d %d", cw.sentenceID, df.getOffset());
            if (cw.sentenceID == 142903426) {
                log.info("%d %s", cw.sentenceID, cw.content);
                break;
            }
        }
    }
}
