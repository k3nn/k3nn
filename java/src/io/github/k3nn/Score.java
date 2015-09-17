package io.github.k3nn;

import io.github.htools.lib.Log;

/**
 * similarity function that scores the similarity of nodes based on the linear
 * distance over publication time.
 *
 * @author jeroen
 */
public enum Score {

    ;
   public static final Log log = new Log(Score.class);
    public static double days3 = 60 * 60 * 24 * 3;

    /**
     * @param time1
     * @param time2
     * @return linear discount function for the difference between two
     * publication times. A fixed interval of 3 days is used throughout, meaning
     * that two nodes with publication times more than 3 days apart will be
     * assigned 0 similarity.
     */
    public static double timeliness(long time1, long time2) {
        double diff = time1 > time2 ? (time1 - time2) : (time2 - time1);
        return (diff >= days3) ? 0.0 : 1.0 - (diff / days3);
    }
}
