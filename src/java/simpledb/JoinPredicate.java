package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int field1Idx;  // field index of first tuple in predicate
    private int field2Idx;  // field index of second tuple in predicate
    private Predicate.Op op;  // operation to apply to field1 and field2

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        field1Idx = field1;
        field2Idx = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // return field1 from t1 compare to field2 from t2 on op condition
        return t1.getField(field1Idx).compare(op, t2.getField(field2Idx));
    }
    
    public int getField1()
    {
        return field1Idx;
    }
    
    public int getField2()
    {
        return field2Idx;
    }
    
    public Predicate.Op getOperator()
    {
        return op;
    }
}
