package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] fieldInfo;  // to store schema of a tuple (fixed size)

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(fieldInfo).iterator();  // use array list iterator
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        fieldInfo = new TDItem[typeAr.length];  // initialize fieldInfo's size
        for (int i = 0; i < typeAr.length; i++) {  // store field info
            fieldInfo[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        fieldInfo = new TDItem[typeAr.length];  // initialize fieldInfo's size
        for (int i = 0; i < typeAr.length; i++) {  // store field info
            fieldInfo[i] = new TDItem(typeAr[i], null);  // unnamed fields
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        if (fieldInfo == null) {  // fieldInfo/fields not yet instantiated
            return 0;
        }  // else, return num of fields
        return fieldInfo.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (numFields() == 0 || i < 0 || i >= numFields()) {  // out of bounds
            throw new NoSuchElementException("i is not a valid field reference");
        }  // else, get the field name of the ith field
        return fieldInfo[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (numFields() == 0 || i < 0 || i >= numFields()) {  // out of bounds
            throw new NoSuchElementException("i is not a valid field reference");
        }  // else, get the field type of the ith field
        return fieldInfo[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {  // traverse to find name
            String fieldName = getFieldName(i);
            if (fieldName != null && fieldName.equals(name)) {  // found match
                return i;
            }  // else, continue searching
        }  // couldn't find matching name
        throw new NoSuchElementException("no field with matching name found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int i = 0; i < numFields(); i++) {  // traverse each type in tuple
            size += (fieldInfo[i].fieldType).getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Merged List of td1 + td2
        List<TDItem> mergedList = new ArrayList<TDItem>(Arrays.asList(td1.fieldInfo));
        mergedList.addAll(Arrays.asList(td2.fieldInfo));
        // initialize to hold new TupleDesc info
        int newSize = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[newSize];
        String[] fieldAr = new String[newSize];
        for (int i = 0; i < newSize; i++) {  // populate new merged info
            typeAr[i] = mergedList.get(i).fieldType;
            fieldAr[i] = mergedList.get(i).fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (this == o) {  // reference equality
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleDesc otherTd = (TupleDesc) o;
        if (numFields() != otherTd.numFields()) {
            return false;  // do not have same number of items
        }  // else, same num of items.  check if types match
        for (int i = 0; i < numFields(); i++) {
            Type thisType = getFieldType(i);
            Type otherType = otherTd.getFieldType(i);
            if (!thisType.equals(otherType)) {  // Types do not equal
                return false;
            }
        }  // all types match, so true
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String descriptor = "";
        for (int i = 0; i < numFields(); i++) {
            String wrapNum = "[" + i + "]";
            descriptor += getFieldType(i) + wrapNum + "(" + getFieldName(i) +
                            wrapNum + ")";
            if (i != numFields() - 1) {  // not last field
                descriptor += ", ";
            }
        }
        return descriptor;
    }
}
