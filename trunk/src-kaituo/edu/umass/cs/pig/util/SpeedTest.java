package edu.umass.cs.pig.util;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Date;

public class SpeedTest {

    public static void main(String[] args) {
        // Make a reasonable large test object. Note that this doesn't
        // do anything useful -- it is simply intended to be large, have
        // several levels of references, and be somewhat random. We start
        // with a hashtable and add vectors to it, where each element in
        // the vector is a Date object (initialized to the current time),
        // a semi-random string, and a (circular) reference back to the
        // object itself. In this case the resulting object produces
        // a serialized representation that is approximate 700K.
        Hashtable obj = new Hashtable();
        for (int i = 0; i < 100; i++) {
            Vector v = new Vector();
            for (int j = 0; j < 100; j++) {
                v.addElement(new Object[] {
                    new Date(),
                    "A random number: " + Math.random(),
                    obj
                 });
            }
            obj.put(new Integer(i), v);
        } 

        int iterations = 10;

        // Make copies of the object using the unoptimized version
        // of the deep copy utility.
        long unoptimizedTime = 0L;
        for (int i = 0; i < iterations; i++) {
            long start = System.currentTimeMillis();
            Object copy = UnoptimizedDeepCopy.copy(obj);
            unoptimizedTime += (System.currentTimeMillis() - start);

            // Avoid having GC run while we are timing...
            copy = null;
            System.gc();
        }

        // Repeat with the optimized version
        long optimizedTime = 0L;
        for (int i = 0; i < iterations; i++) {
            long start = System.currentTimeMillis();
            Object copy = DeepCopy.copy(obj);
            optimizedTime += (System.currentTimeMillis() - start);

            // Avoid having GC run while we are timing...
            copy = null;
            System.gc();
        }

        System.out.println("Unoptimized time: " + unoptimizedTime);
        System.out.println("  Optimized time: " + optimizedTime);
    }

}

