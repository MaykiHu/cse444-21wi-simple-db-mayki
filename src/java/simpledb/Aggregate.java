package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;  // OpIterator giving us tuples
    private int afieldIdx;  // column which we are computing aggregate
    private int gfieldIdx;  // column we are grouping result (-1 if no group)
    private Aggregator.Op op;  // the aggregation op in use
    private Aggregator aggrtr;  // the type of aggregator
    private OpIterator aggrItr;  // the iterator of the aggregator
    private TupleDesc td;  // schema of tuples output from aggregate

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.child = child;
	    afieldIdx = afield;
	    gfieldIdx = gfield;
	    op = aop;
        Type gbfieldtype;
	    if (gfield == -1) {  // no grouping, make td w/ no group
	        gbfieldtype = null;  // null type for no grouping
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {  // has grouping, make td including group
            gbfieldtype = child.getTupleDesc().getFieldType(gfield);
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{child.getTupleDesc().getFieldName(gfieldIdx),
                            child.getTupleDesc().getFieldName(afieldIdx)});
        }  // Now, create appropriate Aggregator (if Int or String)
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            aggrtr = new IntegerAggregator(gfield, gbfieldtype, afield, op);
        } else {  // Is a string
            aggrtr = new StringAggregator(gfield, gbfieldtype, afield, op);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return gfieldIdx;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    return td.getFieldName(0);  // pos 0: group, pos 1: aggregate
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return afieldIdx;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return td.getFieldName(1);  // pos 0: group, pos 1: aggregate
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    child.open();
	    while (child.hasNext()) {  // Aggregate all children
	        aggrtr.mergeTupleIntoGroup(child.next());
        }
	    aggrItr = aggrtr.iterator();
	    aggrItr.open();
	    super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggrItr.hasNext()) {  // if more tuples
            return aggrItr.next();
        }  // else, no more tuples
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    child.rewind();
	    aggrItr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return td;
    }

    public void close() {
	    super.close();
	    child.close();
	    aggrItr.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    return new OpIterator[]{child};
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
