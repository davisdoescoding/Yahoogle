package cis5550.flame;

import cis5550.tools.Logger;

// flame pair obj, holds 2 strings
public class FlamePair implements Comparable<FlamePair> {
	private static final Logger logger = Logger.getLogger(FlamePair.class);

    String a, b;

    // getter for first elem (a)
    public String _1() {
        // log access 4 debug
        logger.debug("accessing first element: " + a);
        return a;
    }

    // getter 4 second elem (b)
    public String _2() {
    	logger.debug("accessing second element: " + b);
        return b;
    }

    // ctor 4 flame pair, init strings
    public FlamePair(String aArg, String bArg) {
        // set first elem (a)
        a = aArg;
        // set second elem (b)
        b = bArg;
        // log pair createc
        logger.debug("created FlamePair: (" + a + ", " + b + ")");
    }

    // compare pairs (a, b)
    public int compareTo(FlamePair o) {
        // chk if first elems (a) same
        if (_1().equals(o._1())) {
            // compare second elems (b)
        	logger.debug("Comparing second elements: " + _2() + " vs " + o._2());
            return _2().compareTo(o._2());
        } else {
            // compare first elems (a)
        	logger.debug("comparing first elements: " + _1() + " vs " + o._1());
            return _1().compareTo(o._1());
        }
    }

    // conv pair 2 string
    public String toString() {
        // log string conv
    	logger.debug("converting FlamePair to string: (" + a + ", " + b + ")");
        return "(" + a + "," + b + ")";
    }
}
