package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIdx;  // index of group-by field
    private Type gbfieldtype;  // field type of the group-by field
    private int afieldIdx;  // index of aggregate field
    private Op op;  // what aggregation operator to use
    private Map<Field, Integer> gbfieldVals;  // group-by fields and aggVals
    private TupleDesc td;  // schema of tuples we are looking at

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Must aggregate over COUNT");
        }  // aggregation is count
        op = what;
        gbfieldIdx = gbfield;
        this.gbfieldtype = gbfieldtype;
        afieldIdx = afield;
        gbfieldVals = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        td = tup.getTupleDesc();
        Field gbfieldKey;  // the gbfield's associated key
        if (gbfieldIdx == NO_GROUPING) {  // if no grouping
            gbfieldKey = null;  // null for no grouping
        } else { // there is grouping, then gbfieldKey is the gbfield
            gbfieldKey = tup.getField(gbfieldIdx);
        }  // update aggregate
        if (gbfieldVals.containsKey(gbfieldKey)) {  // if already tracking
            // Update count to include this tuple
            gbfieldVals.put(gbfieldKey, gbfieldVals.get(gbfieldKey) + 1);
        } else {  // else, start tracking the count of this field
            gbfieldVals.put(gbfieldKey, 1);  // we're at 1 tuple count!
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // Uses TupleIterator that implements OpIterator
        List<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc newTd;  // the newTd for the itr tuples (based on GROUPING)
        if (gbfieldIdx == NO_GROUPING) {  // if no grouping, (aggregateValue)
            newTd = new TupleDesc(new Type[]{Type.INT_TYPE});
            // Traverse every grouped value
            for (Field gbfieldKey : gbfieldVals.keySet()) {
                Tuple t = new Tuple(newTd);
                t.setField(0, new IntField(gbfieldVals.get(gbfieldKey)));
                tuples.add(t);
            }
        } else {  // (groupValue, aggregateValue)
            newTd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                        new String[]{td.getFieldName(gbfieldIdx),
                            td.getFieldName(afieldIdx)});
            for (Field gbfieldKey : gbfieldVals.keySet()) {
                Tuple t = new Tuple(newTd);
                t.setField(0, gbfieldKey);  // groupValue field type
                t.setField(1, new IntField(gbfieldVals.get(gbfieldKey)));
                tuples.add(t);
            }
        }
        return new TupleIterator(newTd, tuples);
    }

}
