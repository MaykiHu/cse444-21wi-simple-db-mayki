package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;  // transaction running the insert
    private OpIterator child;  // child operator which to read tuples to insert
    private TupleDesc td;  // schema of tuple: ret. count of affected records
    private boolean fetchCalled;  // if fetchNext has been called

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        tid = t;
        this.child = child;
        td = new TupleDesc(new Type[]{Type.INT_TYPE});
        fetchCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        fetchCalled = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        fetchCalled = false;
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
        if (fetchCalled) {  // called more than once
            return null;
        }  // hasn't been called, so return 1-field tuple of # inserted
        int numInserted = 0;  // tracks number inserted
        while (child.hasNext()) {
            try {
                Database.getBufferPool().
                        deleteTuple(tid, child.next());
                numInserted++;  // now inserted one more :)
            } catch (Exception e) {  // some exception occured
                throw new DbException("Could not delete tuple");
            }
        }
        Tuple newTuple = new Tuple(td);
        newTuple.setField(0, new IntField(numInserted));
        fetchCalled = true;  // we've called fetchNext() now
        return newTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Set child only if different children are given
        // Inspired by Project implementation
        if (child != children[0]) {
            child = children[0];
        }
    }

}
