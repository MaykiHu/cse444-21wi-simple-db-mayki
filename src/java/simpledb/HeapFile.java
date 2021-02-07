package simpledb;

import java.io.*;
import java.util.*;

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

    private File file;
    private TupleDesc schema;
    private int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        schema = td;
        id = f.getAbsoluteFile().hashCode();  // init once, unique
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Read-only, throw IllegalArgumentException if page not in file
        // Should any other exceptions occur, throws them under above exception
        try {
            RandomAccessFile f = new RandomAccessFile(file, "r");
            // store page contents (can store up to page size)
            byte[] pageContents = new byte[BufferPool.getPageSize()];
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            f.seek(offset);  // start after pointing to offset
            f.read(pageContents);
            f.close();
            HeapPageId hpId = new HeapPageId(id, pid.getPageNumber());
            return new HeapPage(hpId, pageContents);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not read page");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // spec: IOException if write fails, otherwise write to disk
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        // Store page contents, and offset of where to write
        byte[] pageContents = page.getPageData();
        int offset = BufferPool.getPageSize() * page.getId().getPageNumber();
        f.seek(offset);
        f.write(pageContents);  // should write fail, def: throws IOException
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // throws Db if tuple cannot be added, IO if file cannot rw
        ArrayList<Page> newPages = new ArrayList<Page>();
        for (int pgNo = 0; pgNo < numPages(); pgNo++) {  // traverse pages
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
            if (p.getNumEmptySlots() != 0) {  // found empty slot to add tuple
                p.insertTuple(t);
                newPages.add(p);
                return newPages;
            }  // keep traversing for empty slot on page
        }  // no empty pages to add, so append new page to file
        FileOutputStream os = new FileOutputStream(file, true);  // append mode
        os.write(HeapPage.createEmptyPageData());
        os.close();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new
                HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        p.insertTuple(t);
        newPages.add(p);
        return newPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> newPages = new ArrayList<Page>();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,
                t.getRecordId().getPageId(), Permissions.READ_WRITE);
        // throw DbException if tuple not member or cannot be deleted
        p.deleteTuple(t);
        newPages.add(p);
        return newPages;
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * HeapFileIterator is the private iterator for a HeapFile
     * which implements DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int currPage;  // curr page iterator is on
        private Iterator<Tuple> pgTupleItr;  // iterator for tuple on curr page
        private int numPages;  // num of pages in this file
        // Iterator set to only be able to read tuples (no changing)
        private Permissions PERMISSION = Permissions.READ_ONLY;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            currPage = -1;  // no pages open yet
        }

        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            currPage = 0;  // We've opened, now on first page
            PageId pid= new HeapPageId(id, currPage);
            HeapPage p = (HeapPage) Database.getBufferPool().
                    getPage(tid, pid, PERMISSION);
            pgTupleItr = p.iterator();
            numPages = numPages();
        }

        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currPage == -1) {  // iterator isn't open
                return false;
            } else if (pgTupleItr.hasNext()) {  // tuples available on currPage
                return true;
            }  // check if more tuples on other pages
            currPage++;  // move to next page to find tuples
            while (currPage < numPages) {
                PageId pid= new HeapPageId(id, currPage);
                HeapPage p = (HeapPage) Database.getBufferPool().
                        getPage(tid, pid, PERMISSION);
                pgTupleItr = p.iterator();
                if (pgTupleItr.hasNext()) {  // if tuples found on this page
                    return true;
                }  // no tuples on this page, keep checking subsequent pages
                currPage++;
            }  // case where searched all pages and no more tuples remain:
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // check if itr is open before checking if any more tuples
            if (currPage == -1 || !hasNext()) {  // not opened or no tuples
                throw new NoSuchElementException();
            }  // itr is open, and we do have tuples
            return pgTupleItr.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        public void close() {
            // reset values to before open
            currPage = -1;
            pgTupleItr = null;
            numPages = 0;
        }
    }
}

