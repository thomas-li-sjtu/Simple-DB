package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private int numBuckets;
    private final int[] buckets;
    private int numTuples;

    private final double width;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        // 一共多少个bucket
        this.min = min;
        this.max = max;
        this.numBuckets = buckets;
        this.width = (double) (max - min) / buckets;
        this.buckets = new int[buckets];
        this.numTuples = 0;
    }

    private int getIndex(int v) {
        int idx;
        if (v>max || v<min){
            throw new IllegalArgumentException("this value is out of the boundary, which causes the error.");
        }
        if (v==max){
            idx = numBuckets-1;
        }
        else {
            idx = (int) ((v-min)/width);
        }
        return idx;
    }


    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int idx = getIndex(v);
        buckets[idx]++;
        this.numTuples += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        double selectivity = 0.0;
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            int index = getIndex(v);
            for (int i = 0; i < index; i++) {
                selectivity += (buckets[i] + 0.0) / numTuples;
            }
            selectivity += (buckets[index] * (v - index * width - min)) / (width * numTuples);
            return selectivity;
        }
        // ==; equals operator.
        if (op.equals(Predicate.Op.EQUALS)) {
            if (v < min || v > max) return 0.0;
            return 1.0 * buckets[getIndex(v)] / ((int) width + 1) / numTuples;
        }
        // !=; not equal operator
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        // >; greater than operator
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        // <=; less than or equal operator
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        }
        // >=; greater than or equal operator
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        }
        return 0.0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        int sum = 0;
        for (int bucketHeight : buckets) {
            sum += bucketHeight;
        }
        if (sum == 0) {
            return 0.0;
        } else {
            return 1.0 * sum / this.numTuples;
        }
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
