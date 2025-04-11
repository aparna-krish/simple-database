package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field noGroupingField = new IntField(Aggregator.NO_GROUPING);
    private int gbfield;
    private Type gbfieldtype;
    private String groupName;
    private int afield;
    private Op what;
    private Map<Field, Integer> aggMap;
    private Map<Field, Integer> countMap;
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
     * @param aggMap
     *            the map to store the associated aggregated Field and Tuple
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggMap = new HashMap<>();
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
        
        // create default values
        if (!aggMap.containsKey(key)) {
            if (this.what == Aggregator.Op.MIN) {
                aggMap.put(key, Integer.MAX_VALUE);
            } else if (this.what == Aggregator.Op.MAX) {
                aggMap.put(key, Integer.MIN_VALUE);
            } else if (this.what == Aggregator.Op.SUM || this.what == Aggregator.Op.AVG || this.what == Aggregator.Op.COUNT){
                aggMap.put(key, 0);
                if (!countMap.containsKey(key)) {
                    countMap.put(key, 0);
                }
            }
        }
        
        // add value after aggregate computed over tuple field to map
        int currValue = aggMap.get(key);
        int value = ((IntField)tup.getField(afield)).getValue();
        if (this.what == Aggregator.Op.MIN) {
            if (value < currValue) {
                aggMap.put(key, value);
            }
        } else if (this.what == Aggregator.Op.MAX) {
            if (value > currValue) {
                aggMap.put(key, value);
            }
        } else if (this.what == Aggregator.Op.COUNT) {
            aggMap.put(key, currValue + 1);
        } else if (this.what == Aggregator.Op.AVG) {
            countMap.put(key, countMap.get(key) + 1);
            aggMap.put(key, currValue + value);
        } else if (this.what == Aggregator.Op.SUM) {
            aggMap.put(key, currValue + value);
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
            if (this.what == Aggregator.Op.AVG) {
                tup.setField(0, new IntField(aggMap.get(f) / countMap.get(f)));
            } else {
                tup.setField(0, new IntField(aggMap.get(f)));
            }
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

             // create tuples with aggregate computed for each group
            for (Field f : aggMap.keySet()) {
                Tuple tup = new Tuple(td);
                tup.setField(0, f);
                if (this.what == Aggregator.Op.AVG) {
                    tup.setField(1, new IntField(aggMap.get(f) / countMap.get(f)));
                } else {
                    tup.setField(1, new IntField(aggMap.get(f)));
                }
                tupleList.add(tup);
            }
        }
        return new TupleIterator(td, tupleList);
    }

}