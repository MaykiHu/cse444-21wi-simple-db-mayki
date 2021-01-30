package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;  // the predicate to filter tuples with
    private OpIterator child;  // the child operator's iterator

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();  // return child's tuple desc
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child.hasNext()) {  // if more tuples
            Tuple t = child.next();
            if (p.filter(t)) {  // if predicate applies to this tuple t
                return t;
            }
        }
        return null;  // no more tuples
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};  // return OpIterator[] for this child
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Set children only if different children are given (from Project)
        if (child != children[0]) {
            child = children[0];
        }
    }

}
