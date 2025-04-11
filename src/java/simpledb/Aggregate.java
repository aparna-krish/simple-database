package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Type gType;
    private Type aType;
    private OpIterator tupleIter;
    private OpIterator aggregateIter;
    private Aggregator aggregator;

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
	// some code goes here
        this.tupleIter = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        if (gfield == Aggregator.NO_GROUPING) {
            gType = null;
        } else {
            gType = tupleIter.getTupleDesc().getFieldType(gfield);
        }
        aType = tupleIter.getTupleDesc().getFieldType(afield);
        if (aType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gType, afield, aop);
        } else if (aType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	    if (this.groupField() == Aggregator.NO_GROUPING) {
            return null;
        }
        return tupleIter.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return tupleIter.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        // keep merging tuples into aggregator tupleIter, then open
        super.open();
        tupleIter.open();
        while (tupleIter.hasNext()) {
            aggregator.mergeTupleIntoGroup(tupleIter.next());
        }
        aggregateIter = aggregator.iterator();
        aggregateIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	    if (aggregateIter.hasNext()) {
            return aggregateIter.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        aggregateIter.rewind();
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
	// some code goes here
        TupleDesc tempTd = tupleIter.getTupleDesc();
        String aColumn = aop.toString() + "(" + tempTd.getFieldName(afield) + ")";
        String gColumn; 
        if (gfield != Aggregator.NO_GROUPING) {
            gColumn = tempTd.getFieldName(gfield);
        } else {
            gColumn = null;
        }

        String[] fields;
        Type[] types;
        if (gfield == Aggregator.NO_GROUPING) {
            fields = new String[1];
            fields[0] = aColumn;
            types = new Type[1];
            types[0] = Type.INT_TYPE;
        } else {
            fields = new String[2];
            fields[0] = aColumn;
            fields[1] = gColumn;
            types = new Type[2];
            types[0] = gType;
            types[1] = Type.INT_TYPE;
        }
        return new TupleDesc(types, fields);
    }

    public void close() {
	// some code goes here
        tupleIter.close();
        aggregateIter.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	    OpIterator[] children = new OpIterator[1];
        children[0] = aggregateIter;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        aggregateIter = children[0];
    }
    
}