package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    Map<PageId, Lock> pageLocks;
    Map<TransactionId, Set<PageId>> tidPages;
    Map<TransactionId, Set<TransactionId>> dependencies;

    public LockManager() {
        this.pageLocks = new ConcurrentHashMap<PageId, Lock>();
        this.tidPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        this.dependencies = new ConcurrentHashMap<TransactionId, Set<TransactionId>>();
    }

    public void readLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        synchronized (this) {
            // Acquire lock on page
            Lock lock = this.acquire(pid);
    
            // Ensure lock is held by this thread
            if (!lock.heldBy(tid)) {
                // If another thread holds lock put in queue
                if (!lock.getHolders().isEmpty() && lock.isExclusive()) {
                    this.dependencies.put(tid, lock.getHolders());
                    if (this.isDeadLocked(tid)) {
                        // Abort transaction if there is deadlock
                        this.dependencies.remove(tid);
                        throw new TransactionAbortedException();
                    }
                }
    
                // Get read lock
                lock.readLock(tid);
    
                this.dependencies.remove(tid);
                this.getPagesHeld(tid).add(pid);
            }
        }
    }

    public void writeLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        synchronized (this) {
            // Aquire lock on page
            Lock lock = this.acquire(pid);
            
            // Ensure lock is held by this thread
            if (!(lock.heldBy(tid))) {

                // If another thread holds lock, put in queue
                if (!lock.getHolders().isEmpty()){
                    this.dependencies.put(tid, lock.getHolders());

                    // Abort transaction if there is deadlock
                    if (this.isDeadLocked(tid)) {
                        this.dependencies.remove(tid);
                        throw new TransactionAbortedException();
                    }
                }
                
                // Get write lock
                lock.writeLock(tid);
                
                this.dependencies.remove(tid);
                this.getPagesHeld(tid).add(pid);
            }
        }
    }

    public synchronized void release(TransactionId tid, PageId pid) {

        if (this.pageLocks.containsKey(pid)) {
            Lock lock = this.pageLocks.get(pid);
            lock.unlock(tid);
            this.tidPages.get(tid).remove(pid);
        }
    }

    public synchronized void releaseAll(TransactionId tid) {

        if (this.tidPages.containsKey(tid)) {

            // Make copy to avoid ConcurrentModificationException for HeapWriteTest
            Set<PageId> pagesHeld = new HashSet<>(this.tidPages.get(tid));

            for (PageId pid : pagesHeld) {
                this.release(tid, pid);
            }

            this.tidPages.remove(tid);
        }
    }

    private Lock acquire(PageId pid) {

        if (!this.pageLocks.containsKey(pid)) {
            this.pageLocks.put(pid, new Lock());
        }
        return this.pageLocks.get(pid);
    }

    private Set<PageId> getPagesHeld(TransactionId tid) {

        if (!this.tidPages.containsKey(tid)) {

            this.tidPages.put(tid, new HashSet<PageId>());
        }
        return this.tidPages.get(tid);
    }

    private boolean isDeadLocked(TransactionId tid) {

        Set<TransactionId> visited = new HashSet<>();
        List<TransactionId> inProgress = new ArrayList<>();
    
        visited.add(tid);
        inProgress.add(tid);

        for (TransactionId curr: inProgress) {

            if (this.dependencies.containsKey(curr)) {

                // Iterate through transaction queue
                for (TransactionId next: this.dependencies.get(curr)) {

                    // Skip current thread
                    if (!next.equals(curr)) {

                        // If not visited, add to set and keep going
                        if (!visited.contains(next)) {
                            visited.add(next);
                            inProgress.add(next);
                        
                        // If transaction queued before and after
                        } else {

                            return true;
                        }
                    }
                }
            }
    
            // Mark curr transaction as no longer in-progress
            inProgress.remove(curr);
        }

        return false;
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return this.tidPages.containsKey(tid)
                && this.tidPages.get(tid).contains(pid);
    }

    public Set<PageId> getPagesHeldBy(TransactionId tid) {
        if (this.tidPages.containsKey(tid)) {

            return this.tidPages.get(tid);
        }

        return null;
    }
}