package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field noGroupingField = new IntField(Aggregator.NO_GROUPING);
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> counts;
    private String groupName = "";

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Aggregator.Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT operation");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            key = noGroupingField;
        } else {
            key = tup.getField(this.gbfield);
            this.groupName = tup.getTupleDesc().getFieldName(gbfield);
        }

        if (!counts.containsKey(key)) {
            counts.put(key, 0);
        }
        counts.put(key, counts.get(key) + 1);
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
        // some code goes here
        TupleDesc td;
        List<Tuple> tupleList = new ArrayList<>();

        if (this.gbfield == Aggregator.NO_GROUPING) {
            // create tuple desc
            Type[] types = new Type[1];
            String[] fields = new String[1];
            types[0] = Type.INT_TYPE;
            fields[0] = this.what.toString();
            td = new TupleDesc(types, fields);
            
            // create tuple with aggregate computed (no groups)
            Tuple tup = new Tuple(td);
            Field f = noGroupingField;
            tup.setField(0, new IntField(counts.get(f)));
            tupleList.add(tup);
        } else {
            // create tuple desc
            Type[] types = new Type[2];
            String[] fields = new String[2];
            types[0] = gbfieldtype;
            types[1] = Type.INT_TYPE;
            fields[0] = groupName;
            fields[1] = this.what.toString();
            td = new TupleDesc(types, fields);

            // create grouping tuples with aggregate computed for each group
            for (Field f : counts.keySet()) {
                Tuple tup = new Tuple(td);
                tup.setField(0, f);
                tup.setField(1, new IntField(counts.get(f)));
                tupleList.add(tup);
            }
        }
        return new TupleIterator(td, tupleList);
    }

}