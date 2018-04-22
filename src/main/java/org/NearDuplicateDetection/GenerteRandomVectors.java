package org.NearDuplicateDetection;

import java.util.*;
import java.lang.*;

public class GenerteRandomVectors {

    private long seeds[];
    private int sigLen;
    private int vecLen;
    private ArrayList<ArrayList<Double>> randomVectors = new ArrayList<ArrayList<Double>>();

    public GenerteRandomVectors(int sigLen, int vecLen, long seeds[]) {
        this.seeds = seeds;
        this.sigLen = sigLen;
        this.vecLen = vecLen;

        for (int i = 0; i < sigLen; i++) {
            ArrayList<Double> v = generateUnitRandomVector(vecLen, seeds[i]);
            randomVectors.add(v);
        }
    }

    public ArrayList<ArrayList<Double>> getRandomVectors() {
        return randomVectors;
    }    

    public static ArrayList<Double> generateUnitRandomVector(int length, Long seed) {

        double x, y, z;
        Random r = new Random(seed);
        ArrayList<Double> vector = new ArrayList<Double>(length);

        double normalizationFactor = 0;
        for (int i = 0; i < length; i++) {
        // find a uniform random point (x, y) inside unit circle
            do {
                x = 2.0 * r.nextDouble() - 1.0;
                y = 2.0 * r.nextDouble() - 1.0;            
                z = x * x + y * y;
            } while (z > 1 || z == 0); // loop executed 4 / pi = 1.273.. times on average
            // http://en.wikipedia.org/wiki/Box-Muller_transform

            // apply the Box-Muller formula to get standard Gaussian z
            double f = (double) (x * Math.sqrt(-2.0 * Math.log(z) / z));
            normalizationFactor += Math.pow(f, 2.0);
            vector.set(i, f);
        }

        /* normalize vector */
        normalizationFactor = Math.sqrt(normalizationFactor);
        for (int i = 0; i < length; i++) {
          double val = vector.get(i);
          double newf = (double) (val / normalizationFactor);
          vector.set(i, newf);
        }
        
        return vector;
    }
}
