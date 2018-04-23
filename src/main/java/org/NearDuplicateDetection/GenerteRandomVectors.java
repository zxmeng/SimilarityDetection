package org.NearDuplicateDetection;

import java.util.*;
import java.lang.*;

public class GenerteRandomVectors {

    private long seeds[];
    private int sigLen;
    private int vecLen;
    private ArrayList<ArrayList<Double>> randomVectors = new ArrayList<ArrayList<Double>>();

    public GenerteRandomVectors(int vecLen, int sigLen, long seeds[]) {
        this.vecLen = vecLen;
        this.sigLen = sigLen;
        this.seeds = seeds;

        for (int i = 0; i < sigLen; i++) {
            ArrayList<Double> v = generateUnitRandomVector(seeds[i]);
            randomVectors.add(v);
        }
    }

    public ArrayList<ArrayList<Double>> getRandomVectors() {
        return randomVectors;
    }    

    private ArrayList<Double> generateUnitRandomVector(Long seed) {

        double x, y, z;
        Random r = new Random(seed);
        ArrayList<Double> vector = new ArrayList<Double>();

        double normalizationFactor = 0;
        for (int i = 0; i < vecLen; i++) {
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
            vector.add(f);
        }

        /* normalize vector */
        normalizationFactor = Math.sqrt(normalizationFactor);
        for (int i = 0; i < vecLen; i++) {
          double val = vector.get(i);
          double newf = (double) (val / normalizationFactor);
          vector.set(i, newf);
        }
        
        return vector;
    }

    public static void main(String[] argv) throws Exception {
        ArrayList<ArrayList<Double>> randomVectors = new ArrayList<ArrayList<Double>>();
        long[] rseed = new long[100];
        Random r = new Random(1123456);
        for (int i = 0; i < 100; i++) {
            rseed[i] = r.nextLong();
        }
        randomVectors = new GenerteRandomVectors(400, 100, rseed).getRandomVectors();
    }
}
