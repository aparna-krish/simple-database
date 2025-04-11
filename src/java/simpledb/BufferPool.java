package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private Map<PageId, Page> bufferPool;

    private int pagesLimit;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pagesLimit = numPages;
        this.bufferPool = new ConcurrentHashMap<PageId, Page>();
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
      return PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (perm == Permissions.READ_WRITE){
            lockManager.writeLock(tid, pid);        
        } else {
            lockManager.readLock(tid, pid);
        }
        synchronized (this) {
            if (bufferPool.containsKey(pid)) {
                return bufferPool.get(pid);
            }
    
            if (bufferPool.size() >= this.pagesLimit) {
               this.evictPage();
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newPage = dbFile.readPage(pid);
            bufferPool.put(pid, newPage);
            return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry : bufferPool.entrySet()) {

            PageId pid = entry.getKey();
            Page p = entry.getValue();
        
            if (p != null) {
    
                if (commit && this.holdsLock(tid, pid)) {
                    p.setBeforeImage();
                }
                
                TransactionId dirtied = p.isDirty();

                if (dirtied != null && tid.equals(dirtied)) {
                    try {
                        if (commit) {
                            flushPage(pid);
                        } else {
                            discardPage(pid);
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        
        lockManager.releaseAll(tid);
    }


    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
            ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);

            for (Page p : pages) {
                p.markDirty(true, tid);
                
                synchronized (this.bufferPool) {
                    bufferPool.put(p.getId(), p);
                }
            }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException, IOException {
            int tableId = t.getRecordId().getPageId().getTableId();
            ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
    
            for (Page p : pages) {
                p.markDirty(true, tid);
                bufferPool.put(p.getId(), p);
            }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: bufferPool.keySet()) {
            this.flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

        if (bufferPool.containsKey(pid)) {
            HeapPage hp = (HeapPage) bufferPool.get(pid);
            
            // append an update record to the log, with
            // a before-image and after-image.
            TransactionId dirtier = hp.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, hp.getBeforeImage(), hp);
                Database.getLogFile().force();
            }

            file.writePage(hp);
            hp.setBeforeImage();
            hp.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
        if (lockManager.getPagesHeldBy(tid) == null) return;
        for (PageId pid: lockManager.getPagesHeldBy(tid)) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId[] pids = bufferPool.keySet().toArray(new PageId[bufferPool.keySet().size()]); 
        Random rand = new Random(); 
        int ri = rand.nextInt(bufferPool.keySet().size()); 
        PageId pid = pids[ri];
        try {
            flushPage(pid);
        } catch (IOException e) {
            throw new DbException("Can't evict this page");
        }
        this.discardPage(pid);
    }
}