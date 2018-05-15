package com.lordjoe.distributed.spark;

import com.lordjoe.distributed.*;
import com.lordjoe.distributed.spark.accumulators.*;
import org.apache.spark.*;
import org.apache.spark.util.AccumulatorV2;

import java.util.*;

/**
 * com.lordjoe.distributed.spark.SpectrumScoringAccumulator
 * track the number and names of for the scoring of a peptide with a specific ID
 * User: Steve
 * Date: 11/24/2014
 */
public class SpectrumScoringAccumulator extends IAccumulator<SpectrumScoringAccumulator> {

     public static SpectrumScoringAccumulator empty() {
        return new SpectrumScoringAccumulator();
     }


    // key is the machine MAC address
    public final String scoredID;
    private final Map<String, Long> items = new HashMap<String, Long>();
    private long totalCalls;  // number function calls

    private  SpectrumScoringAccumulator() {
        scoredID = null;
    }
    /**
     * will be called to count use on a single machine
     */
    public SpectrumScoringAccumulator(String id) {
        scoredID = id;
    }
    /**
     * will be called to count use on a single machine
     */
    public SpectrumScoringAccumulator(SpectrumScoringAccumulator id)
    {
        scoredID = id.scoredID;
        items.putAll(id.items);
        totalCalls = id.totalCalls;
    }


    /**
     * add the accumulated data to another instance
     *
     * @param added
     * @return
     */
    @Override
    public void add(final SpectrumScoringAccumulator added) {
           addAll(added);
      }


    @Override
    public boolean isZero() {
        return items.isEmpty();
    }

    @Override
    public IAccumulator<SpectrumScoringAccumulator> copy() {
        return new SpectrumScoringAccumulator(this);
    }

    @Override
    public void merge(AccumulatorV2<SpectrumScoringAccumulator, SpectrumScoringAccumulator> otherX) {
        SpectrumScoringAccumulator  other = (SpectrumScoringAccumulator)otherX.copy();
        items.putAll(other.items);
        totalCalls += other.totalCalls;

    }

    @Override
    public void reset() {
        items.clear();
        totalCalls = 0;

    }

    @Override
    public SpectrumScoringAccumulator value() {
        return new SpectrumScoringAccumulator(this);
    }

    /**
     * given a value return it as 0
     * default behavior is to return the value itself
     *
     * @return
     */
    @Override
    public SpectrumScoringAccumulator asZero() {
        return null;
    }

    /**
     * like toString but might add more information than a shorter string
     * usually implemented bu appending toString
     *
     * @param out
     */
    @Override
    public void buildReport(final Appendable out) {

    }

    /**
     * will be called to count use on a single machine
     */
    public SpectrumScoringAccumulator(String id,String peptide,long value ) {
        this(id);
        addEntry(peptide,value);
    }


    protected void validateId() {
        if(this.scoredID == null)
            throw new IllegalStateException("no scored Id");
    }

    protected void validateId(SpectrumScoringAccumulator test) {
          if(!this.scoredID.equals(test.scoredID))
              throw new IllegalStateException("bad scored Id");
      }

    protected void addEntry(String peptide,long value) {
        validateId( );  // id better not be null

        long present = 0;
        if (items.containsKey(peptide))
            present += items.get(peptide);
         items.put(peptide,present + value);
        }


    protected void addAll(SpectrumScoringAccumulator added) {
        validateId(added);

        for (String t : added.items.keySet()) {
            long value = added.get(t);
            addEntry(t, value);
        }
      }


    public long get(String item) {
        if (items.containsKey(item)) {
            return items.get(item);
        }
        return 0;
    }

    public long getTotalCalls() {
        if (totalCalls == 0)
            totalCalls = computeTotal();
        return totalCalls;
    }

    public long computeTotal() {
        long sum = 0;
        for (Long v : items.values()) {
            sum += v;
        }
        return sum;
    }





    public int size() {
        return items.size();
    }

    /**
     * return counts with high first
     *
     * @return
     */
    public List<CountedItem> asCountedItems() {
        List<CountedItem> holder = new ArrayList<CountedItem>();
        for (String s : items.keySet()) {
            holder.add(new CountedItem(s, items.get(s)));
        }
        Collections.sort(holder);
        return holder;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" totalCalls:");
        long total1 = getTotalCalls();
        sb.append(SparkUtilities.formatLargeNumber(total1));
        sb.append("\n");


        sb.append(" totalCalls entries:");
        sb.append(size());
        sb.append("\n");

        List<CountedItem> items = asCountedItems();
        for (CountedItem item : items) {
            sb.append(item.toString());
            sb.append("\n");
        }

        return sb.toString();
    }


}
