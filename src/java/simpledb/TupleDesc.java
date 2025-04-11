package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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

    private List<TDItem> fieldList;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        
        // My code
        return this.fieldList.iterator();
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
        // some code goes here
        // My code
        this.fieldList = new ArrayList<TDItem>();

        // Check if typeAr length and fieldAr are same length
        if (typeAr.length == fieldAr.length) {
            // add TDItem for each type, add names if there
            for (int i = 0; i < typeAr.length; i++) {
                TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
                this.fieldList.add(tdItem);
            }
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
        // some code goes here
        // My code
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        // My code
        return this.fieldList.size();
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
        // some code goes here
        // My code
        if (i < 0 || i >= this.fieldList.size()) {
            throw new NoSuchElementException("Index not in fields");
        }
        return this.fieldList.get(i).fieldName;
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
        // some code goes here
        // My code
        if (i < 0 || i >= this.fieldList.size()) {
            throw new NoSuchElementException("Index not in fields");
        }
        return this.fieldList.get(i).fieldType;
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
        // some code goes here
        // My code
        if (name == null) {

            for (int i = 0; i < this.fieldList.size(); i++) {
                if (this.fieldList.get(i).fieldName == null) {
                    return i;
                }
            }
        }

        else {

            for (int i = 0; i < this.fieldList.size(); i++) {
                if (name.equals(this.fieldList.get(i).fieldName)) {
                    return i;
                }
            }
        }

        throw new NoSuchElementException("Field name not found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // My code
        // For each field, get type and get bytes associated with type
        int size = 0;
        for (int i = 0; i < this.fieldList.size(); i++) {
            size += this.fieldList.get(i).fieldType.getLen();
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
        // some code goes here
        // My code
        // Same logic as constructor, but combined arrays
        // Create new Type array
        Type[] typeAr = new Type[td1.fieldList.size() + td2.fieldList.size()];
        // Create new name array
        String[] fieldAr = new String[td1.fieldList.size() + td2.fieldList.size()];

        // Add td1 details
        for (int i = 0; i < td1.fieldList.size(); i++) {
            typeAr[i] = td1.fieldList.get(i).fieldType;
            fieldAr[i] = td1.fieldList.get(i).fieldName;
        }

        // Add td2 details
        for (int i = 0; i < td2.fieldList.size(); i++) {
            typeAr[i + td1.fieldList.size()] = td2.fieldList.get(i).fieldType;
            fieldAr[i + td1.fieldList.size()] = td2.fieldList.get(i).fieldName;
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
        // some code goes here
        // My code
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc tupDesc = (TupleDesc) o;
        if (this.numFields() != tupDesc.numFields()) {
            return false;
        }
        for (int i = 0; i < this.numFields(); ++i) {
            if (!this.getFieldType(i).equals(tupDesc.getFieldType(i))) {
                return false;
            }
        }
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
        // some code goes here
        // My code
        String str = this.fieldList.get(0).fieldType + "(" + this.fieldList.get(0).fieldName + ")";
        for (int i = 1; i < this.fieldList.size(); i++) {
            str += ", " + this.fieldList.get(i).fieldType + "(" + this.fieldList.get(i).fieldName + ")";
        }
        return str;
    }
}
