package simpledb;

import java.io.*;
import java.util.*;
import java.math.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        // some code goes here
        try {
            byte[] data = new byte[BufferPool.getPageSize()];
            RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
            long position = pid.getPageNumber() * (long) BufferPool.getPageSize();

            // Check that page is in file
            if (position < 0 || position >= raf.length()) {
                throw new IllegalArgumentException("This page isn't in this file.");
            }

            // Set pointer position
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            // Fill data array
            raf.read(data);
            raf.close();

            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
            raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            raf.write(page.getPageData());
            raf.close();
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        numPages = (int) Math.ceil(this.f.length() / BufferPool.getPageSize());
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if (t == null) {
            throw new DbException("TransactionId is null");
        }
        // some code goes here
        ArrayList<Page> result = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            HeapPageId pid = new HeapPageId(this.getId(), i);
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (hp.getNumEmptySlots() > 0) {
                hp.insertTuple(t);
                hp.markDirty(true, tid);
                result.add(hp); 
                return result;
            }
        }
        
        	
		HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
		HeapPage hp = new HeapPage(pid, HeapPage.createEmptyPageData());
        
		hp.insertTuple(t);
		writePage(hp);
        result.add(hp);
        return result;
    }

    //To remove a tuple, you will need to implement deleteTuple.
    // Tuples contain RecordIDs which allow you to find the page 
    //they reside on, so this should be as simple as locating the page 
    //a tuple belongs to and modifying the headers of the page appropriately.
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> result = new ArrayList<>();
        //getPage 
        // RecordId rid = t.getRecordId();
        // PageId pid = rid.getPageId();
        try {
            BufferPool bp = Database.getBufferPool();
            bp.deleteTuple(tid, t);
            PageId pid = t.getRecordId().getPageId();
            Page p = bp.getPage(tid, pid, Permissions.READ_ONLY);
            result.add((Page) p);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
        
        // 
        //p.markDirty()
        return result;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

    public class HeapFileIterator implements DbFileIterator {

        private final TransactionId tid;
        private final HeapFile hf;
        private Iterator<Tuple> tupIter;
        private int currPage;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            this.hf = hf;
            this.tupIter = null;
            this.currPage = 0;
        }
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            HeapPage pageIter = null;
            this.currPage = 0;
            if (this.currPage < this.hf.numPages()) {
                pageIter = (HeapPage) Database.getBufferPool()
                                            .getPage(this.tid,
                                            new HeapPageId(this.hf.getId(), this.currPage),
                                            Permissions.READ_ONLY);
            }
            this.tupIter = pageIter.iterator();
        }
            

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws DbException, TransactionAbortedException {

            //Check if open() has been called && there are pages to iterate over
            if (this.tupIter == null || this.currPage >= this.hf.numPages()) {
                return false;
            }

            // Check if page has been iterated over
            while (!this.tupIter.hasNext()) {
                if (this.currPage >= this.hf.numPages() - 1) {
                    return false;
                }
                // Advance page and get iterator for next page
                currPage++;
                HeapPageId pid = new HeapPageId(hf.getId(), currPage);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                tupIter = page.iterator();
            }
            return tupIter.hasNext();
            
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.hasNext()) {
                throw new NoSuchElementException("No more tuples left");
            }

            return this.tupIter.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            this.currPage = 0;
            this.tupIter = null;
        }
    }


}