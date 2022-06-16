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
import java.lang.reflect.Field;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;

    private OpIterator childOp;

    private int count;

    private boolean hasDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.transactionId = t;
        this.childOp = child;
        this.count = 0;
        this.hasDeleted = false;  // 防止重复操作
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.childOp.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.childOp.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.hasDeleted) {
            while (this.childOp.hasNext()) {
                Tuple tupleDelete = this.childOp.next();
                try {
                    Database.getBufferPool().deleteTuple(this.transactionId, tupleDelete);
                    this.count += 1;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            tuple.setField(0, new IntField(this.count));
            this.hasDeleted = true;
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
