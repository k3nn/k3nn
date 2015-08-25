package kbaeval;

import io.github.htools.io.Datafile;
import io.github.htools.lib.Log;
/**
 *
 * @author jeroen
 */
public class listtopics {
   public static final Log log = new Log( listtopics.class );

    public static void main(String[] args) {
        TopicFile tf = new TopicFile(new Datafile(args[0]));
        for (TopicWritable t : tf) {
            log.info("%d %s", t.id, t.query);
        }
    }
}
