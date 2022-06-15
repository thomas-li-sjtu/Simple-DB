package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfieldIndex;
    private final int afieldIndex;
    private final Type gbfieldType;
    private AggHandler aggHandler;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldIndex = gbfield;  // 分组依据的字段index
        this.afieldIndex = afield;  // 分组后，对什么字段进行聚合
        this.gbfieldType = gbfieldtype;  // 分组依据的字段的类型

        if (what == Op.COUNT) {
            this.aggHandler = new CountHandler();
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

        abstract void handler(Field gbField, StringField aField);
    }

    public class CountHandler extends AggHandler {
        void handler(Field gbField, StringField aField) {
            if (this.agg.containsKey(gbField)) {
                this.agg.put(gbField, this.agg.get(gbField) + 1);
            } else {
                this.agg.put(gbField, 1);
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
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
        StringField aField = (StringField) tup.getField(this.afieldIndex);
        this.aggHandler.handler(gbField, aField); // 通过map分组，通过hander聚合。key:field中已经包含了分组依据字段的值
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
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
