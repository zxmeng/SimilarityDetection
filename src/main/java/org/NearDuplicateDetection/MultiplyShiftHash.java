package org.NearDuplicateDetection;

import java.util.Random;

/**
* Following the "hashing vectors" scheme found here:
* http://en.wikipedia.org/wiki/Universal_hashing
*
* Creates a family of hashes that map a vector of longs
* 
* The family H is initialized with a vector of random seeds, one for each
* hash function. (So |H| = |seeds|.)
* 
* Each call to hash(x) returns a vector v here v_i = h_i(x).
* 
*/

public class MultiplyShiftHash {

    long []seeds;
    int MAXLENGTH = 10000;
    long acoeffmatrix[][]; 
    long bcoeffmatrix[][]; 
    int numHashBits;

    /**
    * Initializes family of |seeds| hash functions according to seeds. Each
    * hash output is of size m bits.
    * 
    * @param numHashBits output size of the hash
    * @param seeds random seeds
    */
    public MultiplyShiftHash(int numHashBits, long seeds[]){
        this.seeds = seeds;
        this.numHashBits = numHashBits;
        acoeffmatrix = new long[seeds.length][MAXLENGTH];
        bcoeffmatrix = new long[seeds.length][MAXLENGTH];
        for (int i = 0; i < seeds.length; i++){
            Random r = new Random(seeds[i]);
            for (int j = 0; j < MAXLENGTH; j++){
                acoeffmatrix[i][j] = r.nextLong();
                bcoeffmatrix[i][j] = r.nextLong();
            }
        }
    }

    public long[] hash(long []v){
        long hashvec[] = new long[seeds.length];

        for (int s = 0; s < seeds.length; s++){
            long sum = 0;
            for (int i = 0; i < v.length && i < MAXLENGTH; i++){
                long a = acoeffmatrix[s][i];
                long b = bcoeffmatrix[s][i];
                if (a % 2 == 0){
                    a += 1;
                }
                sum += v[i]*a + b;
            }
            hashvec[s] = sum >>> (64 - numHashBits);
        }

        return hashvec;
    }

    public long hash(long []v, int i){
        Random r = new Random(seeds[i]);

        long sum = 0;
        for (int j = 0; j < v.length; j++){
            long a = r.nextLong();
            long b = r.nextLong();
            if (a % 2 == 0){
                a += 1;
            }
            sum += v[j]*a + b;
        }

        return (sum >>> (64 - numHashBits));
    }

    public long[] hash(String str){
        byte b[] = str.getBytes();
        int longbytes = Long.SIZE/8;
        long[] v = new long[b.length/longbytes + 1];

        for (int i = 0; i < b.length; i++){
            v[i/longbytes] |= (Byte.valueOf(b[i]) & 0xff) << (longbytes * (i % longbytes));
        }

        return hash(v);
    }

    public long hash(String str, int i){
        byte b[] = str.getBytes();
        long[] v = new long[b.length/Long.SIZE + 1];

        for(int j=0;j<b.length;j++){
            v[j/Long.SIZE] |= (Byte.valueOf(b[j]).hashCode() & 0xff) << (8*(j%Long.SIZE));
        }

        return hash(v, i);
    }
}
