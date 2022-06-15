package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;

    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        // an open method should call any child iterator open methods
        super.open(); // 打开Operator
        this.child.open(); // 打开OpIterator
    }

    public void close() {
        // some code goes here
        // an iterator should call its children's close methods.
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (this.child.hasNext()) {
            Tuple curTuple = this.child.next();
            if (this.p.filter(curTuple)) { // apply the predicate to next tuple
                return curTuple;  // 如果符合，则返回这个tuple
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};  // 参考Operator.java中的注释
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children == null) {
            return;
        }
        this.child = children[0];  // 参考Operator.java中的注释
    }

}
