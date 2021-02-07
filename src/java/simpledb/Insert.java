package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;  // transaction running the insert
    private OpIterator child;  // child operator which to read tuples to insert
    private int tableId;  // table in which to insert tuples
    private TupleDesc td;  // schema of tuple: ret. count of affected records
    private boolean fetchCalled;  // if fetchNext has been called

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        tid = t;
        this.child = child;
        this.tableId = tableId;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetchCalled) {  // called more than once
            return null;
        }  // hasn't been called, so return 1-field tuple of # inserted
        int numInserted = 0;  // tracks number inserted
        while (child.hasNext()) {
            try {
                Database.getBufferPool().
                        insertTuple(tid, tableId, child.next());
                numInserted++;  // now inserted one more :)
            } catch (Exception e) {  // some exception occured
                throw new DbException("Could not insert tuple");
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
