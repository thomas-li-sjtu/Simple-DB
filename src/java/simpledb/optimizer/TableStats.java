package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;

    private int ioCostPerPage;

    private int numTuples;

    private DbFile dbFile;

    private int[] min;  // 保存不同field index对应的最小值
    private int[] max;  // 保存不同field index对应的最大值

    private HashMap<Integer, IntHistogram> intHistogramHashMap;  // 保存不同field index对应的intHistogramHashMap
    private HashMap<Integer, StringHistogram> stringHistogramHashMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;

        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        int numFields = this.dbFile.getTupleDesc().numFields();

        this.max = new int[numFields];  // 建立max数组，并初始化
        this.min = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            this.max[i] = Integer.MIN_VALUE;
            this.min[i] = Integer.MAX_VALUE;
        }

        this.stringHistogramHashMap = new HashMap<>();
        this.intHistogramHashMap = new HashMap<>();

        // 开始扫描这个DbFile
        try {
            SeqScan scan = new SeqScan(new TransactionId(), this.tableid);
            scan.open();
            while (scan.hasNext()) {
                Tuple curTuple = scan.next();  // 获得下一个Tuple
                for (int i = 0; i < numFields; i++) {
                    // 检查字段的类型，如果不是StringType就更新最大值和最小值
                    if (!curTuple.getField(i).getType().equals(Type.STRING_TYPE)) {
                        IntField curField = (IntField) curTuple.getField(i);
                        int value = curField.getValue();
                        this.max[i] = Math.max(this.max[i], value);
                        this.min[i] = Math.min(this.min[i], value);
                    }

                }
                this.numTuples += 1;
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        // 实例化各个field的直方图类
        for (int i = 0; i < numFields; i++) {
            if (this.dbFile.getTupleDesc().getFieldType(i).equals(Type.STRING_TYPE)) {
                // field为STRING_TYPE
                this.stringHistogramHashMap.put(i, new StringHistogram(NUM_HIST_BINS));
            } else {
                // field为INT_TYPE
                this.intHistogramHashMap.put(i, new IntHistogram(NUM_HIST_BINS, this.min[i], this.max[i]));
            }
        }

        // 向各个field的直方图填充数据
        try {
            SeqScan scan = new SeqScan(new TransactionId(), this.tableid);
            scan.open();
            while (scan.hasNext()) {
                Tuple curTuple = scan.next();  // 获得下一个Tuple
                for (int i = 0; i < numFields; i++) {
                    // 检查字段的类型，填充数据
                    if (!curTuple.getField(i).getType().equals(Type.STRING_TYPE)) {
                        this.intHistogramHashMap.get(i).addValue(((IntField) curTuple.getField(i)).getValue());
                    } else {
                        this.stringHistogramHashMap.get(i).addValue(((StringField) curTuple.getField(i)).getValue());
                    }
                }
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return ((HeapFile) this.dbFile).numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        if (this.dbFile.getTupleDesc().getFieldType(field).equals(Type.STRING_TYPE)) {
            return this.stringHistogramHashMap.get(field).avgSelectivity();
        } else {
            return this.intHistogramHashMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (this.dbFile.getTupleDesc().getFieldType(field).equals(Type.STRING_TYPE)) {
            return this.stringHistogramHashMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        } else {
            return this.intHistogramHashMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
