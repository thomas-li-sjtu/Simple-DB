package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Map;
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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private final ConcurrentHashMap<PageId, Node<Page>> lruCache;  // 防止锁的占用

    private final Node<Page> head;
    private final Node<Page> tail;  // lruCache的头和尾

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.lruCache = new ConcurrentHashMap<>();
        this.head = new Node<>();
        this.tail = new Node<>();
        this.head.setNext(this.tail);
        this.tail.setPrev(this.head);
    }

    public class Node<T> {
        private T data;
        private Node<T> next;
        private Node<T> prev;

        public Node() {
            this.next = null;
            this.prev = null;
        }

        public Node(T dataVal) {
            this.data = dataVal;
            this.next = null;
            this.prev = null;
        }

        public void setNext(Node<T> next) {
            this.next = next;
        }

        public void setPrev(Node<T> prev) {
            this.prev = prev;
        }

        public T getData() {
            return data;
        }

        public Node<T> getNext() {
            return next;
        }

        public Node<T> getPrev() {
            return prev;
        }
    }

    public void put(PageId pid, Page curPage) {
        // 将curPage放到lruCache的最前面
        Node<Page> curNode = new Node<>(curPage);
        this.lruCache.put(pid, curNode);

        curNode.setNext(this.head.getNext());
        curNode.setPrev(this.head);  // fix bug in lab3 exercise2
        this.head.getNext().setPrev(curNode);  // 不能忽略这一步
        this.head.setNext(curNode);
    }

    public void removeLast() {
        // 淘汰最后一个Node
        Node<Page> lastPage = this.getTail();
        this.lruCache.remove(lastPage.getData().getId());

        Node<Page> prevNode = lastPage.getPrev();
        Node<Page> nextNode = lastPage.getNext();
        prevNode.setNext(nextNode);
        nextNode.setPrev(prevNode);
    }

    public void remove(PageId pageId) {
        // 从中lruCahe中删除一个Page
        // 从字典中删除
        Node<Page> curNode = this.lruCache.get(pageId);
        this.lruCache.remove(pageId);
        // 从链表中删除
        Node<Page> prevNode = curNode.getPrev();
        Node<Page> nextNode = curNode.getNext();
        prevNode.setNext(nextNode);
        nextNode.setPrev(prevNode);
    }

    public Node<Page> getTail() {
        return this.tail.getPrev();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        Node<Page> curNode = this.lruCache.get(pid);
        if (curNode == null) {
            if (this.lruCache.size() == this.numPages) {
                // 淘汰最后一个Node
                this.removeLast();
            }
            this.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            // 从磁盘读出page（page有自己的table id，而table和dbfile一一对应，dbfile是与磁盘交互的接口）
            curNode = this.lruCache.get(pid);  // 将载入buffer pool的page给到curPage
            return curNode.getData();
        } else {
            this.remove(curNode.getData().getId());
            this.put(curNode.getData().getId(), curNode.getData());
            return curNode.getData();
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> curPage = dbFile.insertTuple(tid, t);
        for (Page page : curPage) {
            page.markDirty(true, tid);
            this.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableid = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        List<Page> curPage = dbFile.deleteTuple(tid, t);
        for (Page page : curPage) {
            page.markDirty(true, tid);
            this.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Node<Page>> entry : this.lruCache.entrySet()) {
            if (entry.getValue().getData().isDirty() != null) {
                this.flushPage(entry.getValue().getData().getId());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page dirtyPage = this.lruCache.get(pid).getData();
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(dirtyPage);  // 写入磁盘
        dirtyPage.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Node<Page> node = this.head.getNext();
        this.remove(node.getData().getId());
        try {
            if (node.getData().isDirty() != null) {
                flushPage(node.getData().getId());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.discardPage(node.getData().getId());
    }

}
