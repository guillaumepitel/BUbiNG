package it.unimi.di.law.bubing.util;

import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.io.BinIO;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class IntCountMinSketchUnsafeTest {

    @Test
    public void testSketch() throws IOException, ClassNotFoundException {
        int s = (int) (1 * 1024 * 1024);
        IntCountMinSketchUnsafe cms = new IntCountMinSketchUnsafe((int)s, 3);

        Random rng = new Random();

        ArrayList<Integer> indexes = new ArrayList<Integer>();
        long[] data = new long[s];
        double[] values = new double[s];
        long[] negdata = new long[s];
        int maxVal = 100;
        for (int i = 0; i < s; i++) {
            long v = rng.nextLong();
            data[i] = v;
            indexes.add(i);
            values[i] = Math.abs(rng.nextDouble()); // Generate uniform distribution from 0 to 99
        }
        for (int i = 0; i < s; i++) {
            long v = rng.nextLong();
            negdata[i] = v;
        }


        // CMS
        System.out.println("Incrementing " + s + " items in count min sketch Unsafe with values 0 - " + maxVal);

        long startRHHSTime = System.nanoTime();
        long count = 0;
        for (int p = 0; p < maxVal; p++)
            count += indexes.parallelStream().mapToInt(i ->
            {
                ThreadLocalRandom trng = ThreadLocalRandom.current();
                if (trng.nextDouble() < values[i]) {
                    cms.increment(Longs.toByteArray(data[i]));
                    return 1;
                } else return 0;
            }).sum();
        long endRHHSTime = System.nanoTime();
        System.out.println("CMS add total : " + (double)(endRHHSTime-startRHHSTime)/1000000000.0 + "s ; per increment " + (double)(endRHHSTime-startRHHSTime)/count);

        final File temp = File.createTempFile(ConcurrentCountingMap.class.getSimpleName() + "-", "-temp");
        temp.deleteOnExit();
        BinIO.storeObject(cms, temp);
        final IntCountMinSketchUnsafe ncms = (IntCountMinSketchUnsafe) BinIO.loadObject(temp);

        long startGetRHHSTime = System.nanoTime();
        double error = indexes.parallelStream().mapToDouble(i -> {
            double e = maxVal * values[i] - ncms.get(Longs.toByteArray(data[i]));
            return e * e;
        }).sum();

        System.gc();
        long endGetRHHSTime = System.nanoTime();
        System.out.println("CMS Error get " + Math.sqrt(error)/s);
        assertTrue(Math.sqrt(error)/s < 0.1);
        System.out.println("CMS get per item " + (double)(endGetRHHSTime-startGetRHHSTime)/s);
        assert(error < 0.1);

        long startNegGetRHHSTime = System.nanoTime();
        error = indexes.parallelStream().mapToDouble(i -> {
            double e = ncms.get(Longs.toByteArray(negdata[i]));
            return e* e;
        }).sum();
        assertTrue(Math.sqrt(error)/s < 0.1);
        System.gc();
        long endNegGetRHHSTime = System.nanoTime();
        System.out.println("CMS Error get NEG " + Math.sqrt(error)/s);
        System.out.println("CMS neg get per item " + (double)(endNegGetRHHSTime-startNegGetRHHSTime)/s);

    }
}
