package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;

    private OpIterator childOp;

    private final int tableId;

    private int count;

    private boolean hasInserted;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.childOp = child;
        this.tableId = tableId;
        this.count = 0;
        this.hasInserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        childOp.open();
    }

    public void close() {
        // some code goes here
        super.close();
        childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOp.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.hasInserted) {
            while (this.childOp.hasNext()) {
                Tuple tupleDelete = this.childOp.next();
                try {
                    Database.getBufferPool().insertTuple(this.transactionId, this.tableId, tupleDelete);
                    this.count += 1;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            tuple.setField(0, new IntField(this.count));
            this.hasInserted = true;
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.childOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length > 0) {
            this.childOp = children[0];
        }
    }
}
