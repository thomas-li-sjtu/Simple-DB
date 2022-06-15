package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfieldIndex;
    private final int afieldIndex;
    private final Type gbfieldType;
    private AggHandler aggHandler;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldIndex = gbfield;  // 分组依据的字段index
        this.afieldIndex = afield;  // 分组后，对什么字段进行聚合
        this.gbfieldType = gbfieldtype;  // 分组依据的字段的类型
        // 由于要根据组进行聚合，因此存储分组结果最好使用map
        switch (what) {
            case MAX:
                this.aggHandler = new MaxHandler();
                break;
            case MIN:
                this.aggHandler = new MinHandler();
                break;
            case SUM:
                this.aggHandler = new SumHandler();
                break;
            case COUNT:
                this.aggHandler = new CountHandler();
                break;
            case AVG:
                this.aggHandler = new AvgHandler();
                break;
        }
    }

    private abstract class AggHandler {
        HashMap<Field, Integer> agg;

        public AggHandler() {
            this.agg = new HashMap<>();
        }

        public HashMap<Field, Integer> getAgg() {
            return this.agg;
        }

        abstract void handler(Field gbField, IntField aField);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        if (this.gbfieldIndex == NO_GROUPING) {
            gbField = null;
        } else {
            gbField = tup.getField(this.gbfieldIndex);
        }
        IntField aField = (IntField) tup.getField(this.afieldIndex);
        this.aggHandler.handler(gbField, aField); // 通过map分组，通过hander聚合。key:field中已经包含了分组依据字段的值
    }

    private class SumHandler extends AggHandler {
        void handler(Field gbField, IntField aField) {
            if (this.agg.containsKey(gbField)) {
                this.agg.put(gbField, this.agg.get(gbField) + aField.getValue());
            } else {
                this.agg.put(gbField, aField.getValue());
            }
        }
    }

    public class CountHandler extends AggHandler {
        void handler(Field gbField, IntField aField) {
            if (this.agg.containsKey(gbField)) {
                this.agg.put(gbField, this.agg.get(gbField) + 1);
            } else {
                this.agg.put(gbField, 1);
            }
        }
    }

    private class MinHandler extends AggHandler {
        void handler(Field gbField, IntField aField) {
            if (this.agg.containsKey(gbField)) {
                this.agg.put(gbField, Math.min(this.agg.get(gbField), aField.getValue()));
            } else {
                this.agg.put(gbField, aField.getValue());
            }
        }
    }

    private class MaxHandler extends AggHandler {
        void handler(Field gbField, IntField aField) {
            if (this.agg.containsKey(gbField)) {
                this.agg.put(gbField, Math.max(this.agg.get(gbField), aField.getValue()));
            } else {
                this.agg.put(gbField, aField.getValue());
            }
        }
    }

    private class AvgHandler extends AggHandler {
        HashMap<Field, Integer> count, sum;

        public AvgHandler() {
            this.sum = new HashMap<>();
            this.count = new HashMap<>();
        }

        void handler(Field gbField, IntField aField) {
            if (this.agg.containsKey(gbField)) {
                this.sum.put(gbField, this.sum.get(gbField) + aField.getValue());
                this.count.put(gbField, this.count.get(gbField) + 1);
            } else {
                this.sum.put(gbField, aField.getValue());
                this.count.put(gbField, 1);
            }
            this.agg.put(gbField, this.sum.get(gbField) / this.count.get(gbField));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        HashMap<Field, Integer> aggMap = this.aggHandler.getAgg();
        ArrayList<Tuple> tupleList = new ArrayList<>();

        TupleDesc desc;
        if (this.gbfieldIndex == NO_GROUPING) {
            desc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggValue"});
            Tuple tuple = new Tuple(desc);
            tuple.setField(0, new IntField(aggMap.get(null)));
            tupleList.add(tuple);
        } else {
            desc = new TupleDesc(new Type[]{this.gbfieldType, Type.INT_TYPE}, new String[]{"groupValue", "aggValue"});
            for (Map.Entry<Field, Integer> entry : aggMap.entrySet()) {
                Tuple tuple = new Tuple(desc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tupleList.add(tuple);
            }
        }

        return new TupleIterator(desc, tupleList);
    }

}
