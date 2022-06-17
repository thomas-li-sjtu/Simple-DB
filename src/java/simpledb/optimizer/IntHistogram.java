package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min, max, numBuckets;
    private int buckets[];
    private int numTuples;

    private double width;

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
        this.width = (max - min + 1.0) / buckets;
        this.buckets = new int[buckets];
        this.numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v < this.min || v > this.max) {
            return;
        }
        int index = (int) ((v - this.min) / width);
        this.buckets[index] += 1;
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
        // some code goes here
        switch (op) {
            case EQUALS:
                return this.estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1)
                        - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN:
                return 1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case LESS_THAN_OR_EQ:
                return 1.0 - this.estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                return this.estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case GREATER_THAN:
                if (v <= this.min) {
                    return 1.0;  // 此时整个直方图都算进去
                } else if (v > this.max) {
                    return 0.0;
                } else {
                    int index = (int) ((v - this.min) / this.width);
                    double sum = 0.0;
                    for (int i = index + 1; i < buckets.length; i++) {
                        sum += buckets[i];
                    }
                    double indexCoverage = buckets[index] * ( // v所在的bucket中，大于等于v的tuple数目
                            (min + (index + 1) * width - v - 1) / width
                    );
                    return (sum + indexCoverage) / this.numTuples;
                }
            case NOT_EQUALS:
                return 1.0 - this.estimateSelectivity(Predicate.Op.EQUALS, v);
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
