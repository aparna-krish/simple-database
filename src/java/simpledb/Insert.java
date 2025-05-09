package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private OpIterator childIter;
    private TransactionId tid;
    private int tableId;
    private boolean inserted;

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
        // some code goes here
        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableId);
        if (!tableTd.equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table");
        }
        this.tid = t;
        this.childIter = child;
        this.tableId = tableId;
        this.inserted = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insert td"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        childIter.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        childIter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childIter.rewind();
        inserted = false;
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
        // some code goes here
        if (inserted) {
            return null;
        }
        int count = 0;
        BufferPool bp = Database.getBufferPool();
        while (childIter.hasNext()) {
            Tuple t = childIter.next();
            try {
                bp.insertTuple(tid, tableId, t);
            } catch (Exception e) {
                throw new IllegalArgumentException();
            }
            count++;
        }
        inserted = true;
        Tuple result = new Tuple(this.getTupleDesc());
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.childIter };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.childIter = children[0];
    }
}