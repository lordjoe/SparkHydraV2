package com.lordjoe.distributed.spark;

import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.io.*;

/**
 * com.lordjoe.distributed.spark.Statistics
 * keep statistics  This structure is immutable
 * User: Steve
 * Date: 11/13/2014
 */
public class Statistics extends AccumulatorV2<Statistics,Statistics> implements Serializable {


    public static final Statistics ZERO = new Statistics();
    private   int number;
    private   double sum;
    private   double sumsquare;
    private   double max;
    private   double min;

    public Statistics() {
       reset();
     }

    /**
     * build with 1 or more numbers
     * @param d  first value
     * @param values other values - if any
     */
    public Statistics(double d, double... values) {
        number = 1 + values.length;
        double tsum = d;
        double tsumsq = d * d;
        double tmin = d;
        double tsmax = d;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < values.length; i++) {
            double value = values[i];
            tsum += value;
            tsumsq += value * value;
            tmin = Math.max(value, d);
            tsmax = Math.min(value, d);
         }
         sum = tsum;
        sumsquare = tsumsq;
        max = tmin;
        min = tsmax;
    }

    private Statistics(Statistics s1 ) {
        number = s1.number  ;
        sum = s1.sum  ;
        sumsquare = s1.sumsquare ;
        max = s1.max ;
        min =  s1.min;
    }

    private Statistics(Statistics s1, Statistics s2) {
        number = s1.number + s2.number;
        sum = s1.sum + s2.sum;
        sumsquare = s1.sumsquare + s2.sumsquare;
        max = Math.max(s1.max, s2.max);
        min = Math.min(s1.min, s2.min);
    }

    private Statistics(Statistics s1, double d) {
        number = s1.number + 1;
        sum = s1.sum + d;
        sumsquare = s1.sumsquare + d * d;
        max = Math.max(s1.max, d);
        min = Math.min(s1.min, d);
    }

    public Statistics add(double d) {
        return new Statistics(this, d);
    }

    @Override
    public boolean isZero() {
        return sumsquare == 0;
    }

    @Override
    public AccumulatorV2<Statistics, Statistics> copy() {
        return new Statistics(this);
    }

    @Override
    public void reset() {
        number = 0;
        sum = 0;
        sumsquare = 0;
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;

    }

    public void add(Statistics s2) {
        number +=s2.number;
        sum += s2.sum;
        sumsquare += s2.sumsquare;
        max = Math.max( max, s2.max);
        min = Math.min( min, s2.min);
    }

    @Override
    public void merge(AccumulatorV2<Statistics, Statistics> other) {
        add(other.value()) ;
    }

    @Override
    public Statistics value() {
        return  new Statistics(this);
    }

    public int getNumber() {
        return number;
    }

    public double getSum() {
        return sum;
    }

    @SuppressWarnings("UnusedDeclaration")
    public double getSumsquare() {
        return sumsquare;
    }

    public double getMax() {
        return max;
    }

    @SuppressWarnings("UnusedDeclaration")
    public double getMin() {
        return min;
    }

    public double getAverage() {
        if (number == 0)
            return 0;
        return sum / number;
    }

    public double getStandardDeviation() {
        if (number < 2)
            return Double.MAX_VALUE;
        double variance = (sumsquare - sum * getAverage()) / (number - 1.0);
        //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
        double answer = Math.sqrt(variance);
        return answer;
    }



}
