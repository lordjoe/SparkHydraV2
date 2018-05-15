package com.lordjoe.algorithms;

import com.lordjoe.distributed.spark.accumulators.*;
import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.io.*;
import java.util.*;

/**
 * com.lordjoe.algorithms.CountedDistribution
 *
 * @author Steve Lewis
 * @date 5/28/2015
 */
public class CountedDistribution extends IAccumulator<CountedDistribution> {


    public static CountedDistribution empty() {
        return new CountedDistribution();
    }

    private CountedDistribution() {
    }



    /**
     * convenience methos to make one with one count
     * Spark accumulators like this
     *
     * @param added
     */
    public CountedDistribution(int added) {
        this();
        add(added);
    }

    /**
     * convenience method to make one with one count
     * Spark accumulators like this  -this allows one not to need to cast a long
     *
     * @param added
     */
    public CountedDistribution(long added) {
        this();
        add(added);
    }
    /**
     * convenience method to make one with one count
     * Spark accumulators like this  -this allows one not to need to cast a long
     *
     * @param added
     */
    public CountedDistribution(CountedDistribution copy) {
        this();
        maxValue = copy.maxValue;
        System.arraycopy(bins, 0, copy.bins, 0, bins.length);
    }

    private int maxValue;
    private final int[] bins = new int[31];

    public int getBin(int bin) {
        return bins[bin];
    }

    public int[] getBins() {
        int[] ret = new int[bins.length];
        System.arraycopy(bins, 0, ret, 0, bins.length);
        return ret;
    }


    public void add(int added) {
        added = Math.max(0, added);
        if (added == 0)
            return;
        maxValue = Math.max(added, maxValue);
        int binNumber = 0;
        added >>= 1;
        while (added != 0) {
            binNumber++;
            added >>= 1;
        }
        bins[binNumber]++;
    }

    /**
     * allows one not fo cast a long
     *
     * @param added
     */
    public void add(long added) {
        add((int) added);
    }

    public void add(CountedDistribution added) {
        maxValue = Math.max(added.maxValue, maxValue);

        for (int i = 0; i < bins.length; i++) {
            bins[i] += added.bins[i];
        }
      }

    @Override
    public boolean isZero() {
        return maxValue == 0;
    }

    @Override
    public IAccumulator<CountedDistribution> copy() {
        return new CountedDistribution(this);
    }

    @Override
    public void merge(AccumulatorV2<CountedDistribution, CountedDistribution> other) {
        add((CountedDistribution)other)  ;
    }

    @Override
    public void reset() {
        maxValue = 0;
        for (int i = 0; i < bins.length; i++) {
              bins[i] = 0;

        }
    }

    @Override
    public CountedDistribution value() {
        return new CountedDistribution(this);
    }

    /**
     * given a value return it as 0
     * default behavior os th return the value itself
     *
     * @return
     */
    @Override
    public CountedDistribution asZero() {
        return empty();
    }

    /**
     * like toString but might add more information than a shorter string
     * usually implemented bu appending toString
     *
     * @param out
     */
    @Override
    public void buildReport(final Appendable out) {
        try {
            out.append(toString()) ;
        }
        catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

    public int getMaxValue() {
        return maxValue;
    }

    public int getCount() {
        int ret = 0;
        for (int bin : bins) {
            ret += bin;
        }
        return 0;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Max value " + Long_Formatter.format(getMaxValue()) + " total " + Long_Formatter.format(getCount()));
        sb.append("\n");
        int index = 1;
        for (int i = 0; i < bins.length; i++) {
            int bin = bins[i];
            if (bin > 0) {
                sb.append(Long_Formatter.format(index) + "\t" + Long_Formatter.format(bin));
                sb.append("\n");
            }
            index *= 2;
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CountedDistribution that = (CountedDistribution) o;

        return Arrays.equals(bins, that.bins);

    }

    @Override
    public int hashCode() {
        return bins != null ? Arrays.hashCode(bins) : 0;
    }
}
