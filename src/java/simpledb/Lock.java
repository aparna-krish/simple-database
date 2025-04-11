package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Lock {
    Set<TransactionId> holders;
    Map<TransactionId, Boolean> hasAcquired;
    boolean isExclusive;
    private int readThreads;
    private int writeThreads;

    public Lock() {
        this.holders = new HashSet<TransactionId>();
        this.hasAcquired = new ConcurrentHashMap<TransactionId, Boolean>();
        this.isExclusive = false;
        this.readThreads = 0;
        this.writeThreads = 0;
    }

    public void readLock(TransactionId tid) {
        if (!this.holders.contains(tid) || this.isExclusive) {
            synchronized (this) {
                try {
                    while (this.writeThreads > 0) {
                        this.wait();
                    }
                    ++this.readThreads;
                    this.holders.add(tid);
                    this.isExclusive = false;
                    this.hasAcquired.remove(tid);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.hasAcquired.put(tid, false);
        }
    }

    public void writeLock(TransactionId tid) {

        boolean canAcquireWriteLock = !(this.holders.contains(tid) && this.isExclusive)
        && !(this.hasAcquired.containsKey(tid) && this.hasAcquired.get(tid));
        
        // Acquire lock
        if (canAcquireWriteLock) {
            hasAcquired.put(tid, true);
            synchronized (this) {
                try {
                    // Release lock if another If the thread already holds the write lock, release it
                    if (this.holders.contains(tid)) {
                        while (this.holders.size() > 1) {
                            this.wait();
                        }

                        synchronized (this) {
                                --this.readThreads;
                                this.holders.remove(tid);
                        }
                    }

                    // Wait until no threads are reading or writing
                    while (this.readThreads != 0 || this.writeThreads != 0) {
                        this.wait();
                    }

                    ++this.writeThreads;
                    this.holders.add(tid);
                    this.isExclusive = true;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.hasAcquired.remove(tid);
        }
    }

    public void unlock(TransactionId tid) {
        if (this.holders.contains(tid)){
            synchronized (this) {
                if (this.isExclusive) {
                    --this.writeThreads;
                }
                else {
                    --this.readThreads;
                }
                this.holders.remove(tid);
                notifyAll();
            }
        }
    }
    public Set<TransactionId> getHolders() {
        return this.holders;
    }

    public boolean isExclusive() {
        return this.isExclusive;
    }

    public boolean heldBy(TransactionId tid) {
        return this.holders.contains(tid);
    }

}