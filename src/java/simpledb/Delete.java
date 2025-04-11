package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator childIter;
    private TupleDesc td;
    private boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.childIter = child;
        this.deleted = false;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"delete td"});
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
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (deleted) {
            return null;
        }
        int count = 0;
        BufferPool bp = Database.getBufferPool();
        while (childIter.hasNext()) {
            Tuple t = childIter.next();
            try {
                bp.deleteTuple(tid, t);
            } catch (Exception e) {
                throw new IllegalArgumentException();
            }
            count++;
        }
        deleted = true;
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