package io.github.k3nn;

import io.github.htools.lib.Log;

/**
 * similarity function that scores the similarity of nodes based on cosine
 * similarity between their binary term vectors discounted by the linear
 * distance over publication time.
 * @author jeroen
 */
public enum Score {;
   public static final Log log = new Log( Score.class ); 
   public static double days3 = 60 * 60 * 24 * 3;
   
   /**
    * @param time1
    * @param time2
    * @return linear discount function for the difference between two publication
    * times.
    */
   public static double timeliness(long time1, long time2) {
       double diff = time1 > time2?(time1 - time2):(time2 - time1);
       return (diff >= days3)?0.0:1.0 - (diff / days3);
   }
}
