package simpledb;

import java.io.*;

import java.util.*;  // for List/ArrayList
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    //** Default number in unit: ms for time-out based detection */
    //public static final int TIMEOUT = 1000000;  // changed to 1000s

    private int numPages;  // max num of pages allowed in this buffer pool
    // collection of all pages in this pool
    private ConcurrentHashMap<PageId, Page> pages;
    private LockManager lkManager;  // manage locking

    /**
     * A private help class to facilitate organizing the information of a lock
     * */
    private class Lock {
        // public fields for private access within BufferPool by dot notation
        public TransactionId tid;  // which tid this lock holds
        public boolean isExclusive;  // if this lock is exclusive (T) or shared

        public Lock(TransactionId tid, boolean isExclusive) {
            this.tid = tid;
            this.isExclusive = isExclusive;
        }
    }

    /**
     * A private help class to manage locks and locking/unlocking
     * Method naming inspired by Java API LockManager naming conventions
     * */
    private class LockManager {
        // public fields for private access within BufferPool by dot notation
        // all locked pages stored by PageId
        private ConcurrentHashMap<PageId, List<Lock>> pageLocks;

        public LockManager() {
            pageLocks = new ConcurrentHashMap<PageId, List<Lock>>();
        }

        // Tries to acquire lock; true if obtained, false otherwise
        // @param tid the TransactionId of the transaction involved
        // @param pid the PageId of the page which this lock involves
        // @param isExclusive whether or not this lock is exclusive
        public synchronized boolean acquireLock(TransactionId tid, PageId pid,
                                                boolean isExclusive) {
            // if not locked on pid, add and acquire lock
            if (!pageLocks.containsKey(pid)) {
                List<Lock> newLocks = new ArrayList<Lock>();
                Lock newLock = new Lock(tid, isExclusive);
                newLocks.add(newLock);
                pageLocks.put(pid, newLocks);
                return true;
            }  // else, is locked by some tid (see if for existing transaction)
            List<Lock> lockList = pageLocks.get(pid);  // this pg's locks
            for (Lock lock : lockList) {
                if (lock.tid == tid) {  // if we found the matching tid
                    // if same lock states or existing is exclusive (dominates)
                    if (lock.isExclusive == isExclusive || lock.isExclusive) {
                        return true;
                    } else if (lockList.size() == 1) {  // 1 shared transaction
                        lock.isExclusive = true;  // upgrd. shared to exclusive
                        return true;
                    } else {  // trying to put an exclusive on existing shared!
                        return false;  // not allowed >:(
                    }
                }
            }  // couldn't find matching tid
            // Add only if shared locks on shared locks (no exclusive)
            if (!isExclusive && !lockList.get(0).isExclusive) {
                Lock sharedLock = new Lock(tid, false);
                lockList.add(sharedLock);
                pageLocks.put(pid, lockList);
                return true;
            }
            return false;
        }

        // Tries to release lock; true if released, false otherwise
        // @param tid the TransactionId of the transaction involved
        // @param pid the PageId of the page which this lock involves
        public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
            List<Lock> lockList = pageLocks.get(pid);  // locks on this page
            // Traverse all locks on pg and remove the matching lock
            for (int i = 0; i < lockList.size(); i++) {
                Lock currLock = lockList.get(i);
                if (currLock.tid == tid) {
                    lockList.remove(currLock);
                    // if no more locks on pg after lock release, unmap this pg
                    if (lockList.isEmpty()) {
                        pageLocks.remove(pid);
                    }
                    return true;  // lock is released
                }
            }  // no matching lock found
            return false;
        }

        // Determines whether the lock is held
        // @param tid the TransactionId of the transaction involved
        // @param pid the PageId of the page which this lock involves
        public synchronized boolean isLockHeld(TransactionId tid, PageId pid) {
            if (!pageLocks.containsKey(pid)) {  // if no locks on this pid
                return false;
            }  // else, there are locks on this pid; see if transaction matches
            List<Lock> lockList = pageLocks.get(pid);  // locks on this page
            for (Lock lock : lockList) {  // traverse for matching tid
                if (lock.tid == tid) {
                    return true;  // same lock held matching tid on pid
                }
            }  // made it out -- no matching lock held
            return false;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        pages = new ConcurrentHashMap<PageId, Page>();
        lkManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // Block and acquire lock before accessing to return page:
        // exclusive lock if writing
        boolean isExclusive = perm.equals(Permissions.READ_WRITE);
        //boolean hasLock = false;
        long start = System.currentTimeMillis();
        // Taking out timeout deadlock policy until further
        // optimization; causes failed tests due to timeout
//        while (!hasLock) {
//            // TIMEOUT policy for deadlock (feeling homicidal :P)
//            if (System.currentTimeMillis() - start > TIMEOUT) {
//                throw new TransactionAbortedException();
//            }
//            hasLock = lkManager.acquireLock(tid, pid, isExclusive);
//        }
        lkManager.acquireLock(tid, pid, isExclusive);
        // Now, return page
        if (pages.containsKey(pid)) {  // if page is present
            return pages.get(pid);
        }  // page wasn't present, so look to add
        if (pages.size() >= numPages) {  // if no space to add page
            evictPage();
        }  // Add page that was not present before
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        synchronized (pages) {
            pages.put(pid, file.readPage(pid));
        }
        return pages.get(pid);  // return added page
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
        lkManager.releaseLock(tid, pid);  // we call it :), not the clients
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);  // always commit :P
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lkManager.isLockHeld(tid, p);  // :( why not call it pid not p
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
        if (commit) {  // removed flushPages, instead do log tracking
            for (PageId pid : pages.keySet()) {
                Page p = pages.get(pid);
                if (p.isDirty() == tid) {  // dirtied by this transaction
                    Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
                    Database.getLogFile().force();
                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    p.setBeforeImage();
                }
            }
        } else {  // abort!  don't commit, restore to on-disk state
            for (PageId pid : pages.keySet()) {
                Page p = pages.get(pid);
                if (p.isDirty() == tid) {  // if dirtied by this tid
                    Page diskPage = Database.getCatalog()
                            .getDatabaseFile(pid.getTableId()).readPage(pid);
                    pages.put(pid, diskPage);  // restored to disk page!
                }
            }
        }  // Release locks
        for (PageId pid : pages.keySet()) {  // traverse pages
            if (holdsLock(tid, pid)) {
                releasePage(tid, pid);
            }
        }
    }

    // Private helper method to add dirtied pages to the cache
    // (reduces duplicate code)  :)
    // @param tid the transaction adding the tuple to dirty page
    // @param dirtiedPages the dirtied pages to add to cache
    private void addDirtied(TransactionId tid, List<Page> dirtiedPages) {
        for (Page p : dirtiedPages) {  // mark pages that were dirtied
            p.markDirty(true, tid);
            synchronized (pages) {
                pages.put(p.getId(), p);  // add/replace pg dirtied to cache
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtiedPages = f.insertTuple(tid, t);
        addDirtied(tid, dirtiedPages);  // marks dirty as well :)
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(
                    t.getRecordId().getPageId().getTableId());
        List<Page> dirtiedPages = f.deleteTuple(tid, t);
        addDirtied(tid, dirtiedPages);  // marks dirty as well :)
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: pages.keySet()) {  // traverse pages by id
            flushPage(pid);  // flush, but only if dirty
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p = pages.get(pid);
        TransactionId dirtyId = p.isDirty();
        if (p != null && dirtyId != null) {  // if dirty page is executing
            // append an update record to the log, with
            // a before-image and after-image. (had to modify name to match)
            Database.getLogFile().logWrite(dirtyId, p.getBeforeImage(), p);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId())
                    .writePage(p);  // 1) write dirty page to disk
            p.markDirty(false, null);  // 2) mark written not dirty
        }  // 3) written page left in BufferPool
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : pages.keySet()) {
            Page p = pages.get(pid);
            if (p.isDirty() == tid) {  // dirtied by this transaction
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // order removed not guaranteed :( bc hashmap, keeping this way
        Iterator<PageId> pgItr = pages.keySet().iterator();
        while (pgItr.hasNext()) {
            PageId pid = pgItr.next();  // grab this page id
            if (pages.get(pid).isDirty() == null) {  // page is not dirty
                try {
                    flushPage(pid);
                } catch (IOException e) {
                    throw new DbException("IOException occurred: " +
                            e.getMessage());
                }
                discardPage(pid);  // evict non-dirty page
                return;  // exit
            }  // else, page is dirty so go to next page
        }  // ended up that all pages were dirty!
        throw new DbException("Failed to evict page; all pages dirty! ;-;");
    }

}
