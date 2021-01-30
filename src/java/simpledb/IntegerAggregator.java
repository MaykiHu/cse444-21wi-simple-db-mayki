package simpledb;
import java.util.*;
import java.lang.Math.*;  // For Math.min/max

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIdx;  // index of the group-by field (or NO_GROUPING)
    private Type gbfieldtype;  // the type of the group by field (null if none)
    private int afieldIdx;  // index of aggregate field in the tuple
    private Op op;  // the aggregation operator
    private Map<Field, Integer> gbfieldVals;  // group-by fields and aggrVals
    private Map<Field, Integer> gbfieldCounts;  // for AVG, counts of tuples
    private TupleDesc td;  // schema of tuples we're looking at


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfieldIdx = gbfield;
        this.gbfieldtype = gbfieldtype;
        afieldIdx = afield;
        op = what;
        gbfieldVals = new HashMap<Field, Integer>();
        gbfieldCounts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        td = tup.getTupleDesc();
        Field gbfieldKey;  // the gbfield's associated key
        if (gbfieldIdx == NO_GROUPING) {  // if no grouping
            gbfieldKey = null;  // null for no grouping
        } else { // there is grouping, then gbfieldKey is the gbfield
            gbfieldKey = tup.getField(gbfieldIdx);
        }
        int tupVal = ((IntField)tup.getField(afieldIdx)).getValue();
        switch (op) {  // Based on op for: COUNT, SUM, AVG, MIN, MAX
            case COUNT:
                // If we are already tracking this gbfield
                if (gbfieldVals.containsKey(gbfieldKey)) {
                    gbfieldVals.put(gbfieldKey,
                            gbfieldVals.get(gbfieldKey) + 1);
                } else {  // have to start tracking count of this field
                    gbfieldVals.put(gbfieldKey, 1);
                }
                break;

            case SUM:
                // If we are already tracking this gbfield
                if (gbfieldVals.containsKey(gbfieldKey)) {
                    gbfieldVals.put(gbfieldKey,  // increment sum
                            gbfieldVals.get(gbfieldKey) + tupVal);
                } else {  // have to start tracking count of this field
                    gbfieldVals.put(gbfieldKey, tupVal);
                }
                break;

            case AVG:
                // If we are already tracking this gbfield
                if (gbfieldVals.containsKey(gbfieldKey)) {
                    gbfieldVals.put(gbfieldKey,  // increment sum
                            gbfieldVals.get(gbfieldKey) + tupVal);
                    gbfieldCounts.put(gbfieldKey,
                            gbfieldCounts.get(gbfieldKey) + 1);
                } else {  // have to start tracking count of this field
                    gbfieldVals.put(gbfieldKey, tupVal);
                    gbfieldCounts.put(gbfieldKey, 1);
                }
                break;

            case MIN:
                // If we are already tracking this gbfield
                if (gbfieldVals.containsKey(gbfieldKey)) {
                    gbfieldVals.put(gbfieldKey,  // update minimum
                            Math.min(gbfieldVals.get(gbfieldKey), tupVal));
                } else {  // have to start tracking count of this field
                    gbfieldVals.put(gbfieldKey, tupVal);
                }
                break;

            case MAX:
                // If we are already tracking this gbfield
                if (gbfieldVals.containsKey(gbfieldKey)) {
                    gbfieldVals.put(gbfieldKey,  // update maximum
                            Math.max(gbfieldVals.get(gbfieldKey), tupVal));
                } else {  // have to start tracking count of this field
                    gbfieldVals.put(gbfieldKey, tupVal);
                }
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
                int aggrVal = gbfieldVals.get(gbfieldKey);
                if (op == Op.AVG) {  // if computing avg
                    aggrVal /= gbfieldCounts.get(gbfieldKey);
                }
                t.setField(0, new IntField(aggrVal));
                tuples.add(t);
            }
        } else {  // (groupValue, aggregateValue)
            newTd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{td.getFieldName(gbfieldIdx),
                            td.getFieldName(afieldIdx)});
            for (Field gbfieldKey : gbfieldVals.keySet()) {
                Tuple t = new Tuple(newTd);
                int aggrVal = gbfieldVals.get(gbfieldKey);
                if (op == Op.AVG) {  // if computing avg
                    aggrVal /= gbfieldCounts.get(gbfieldKey);
                }
                t.setField(0, gbfieldKey);  // groupValue field type
                t.setField(1, new IntField(aggrVal));
                tuples.add(t);
            }
        }
        return new TupleIterator(newTd, tuples);
    }

}
