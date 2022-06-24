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
import java.util.Random;
import java.util.Vector;
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

    private final LockManager lockManager;

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

        this.lockManager = new LockManager();
    }

    public static class Lock {
        private TransactionId tid;
        private Permissions permission;

        public Lock(TransactionId tid, Permissions permission) {
            this.permission = permission;
            this.tid = tid;
        }

        public Permissions getPermission() {
            return permission;
        }

        public TransactionId getTid() {
            return tid;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }

        public void setPermission(Permissions permission) {
            this.permission = permission;
        }
    }

    public static class LockManager {
        private final ConcurrentHashMap<PageId, Vector<Lock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        // 删除一个lock，如果对应的Vector为空，则删除这个entry
        public synchronized void removeLock(TransactionId tid, PageId pageId) {
            Vector<Lock> curLockVector = this.lockMap.get(pageId);
            int index = 0;
            boolean found = false;
            for (int i = 0; i < curLockVector.size(); i++) {
                if (curLockVector.get(i).getTid().equals(tid)) {
                    found = true;
                    index = i;
                    break;
                }
            }
            if (found) {
                curLockVector.remove(index);
                if (curLockVector.size() == 0) {
                    this.lockMap.remove(pageId);
                }
            }
        }

        // 删除与一个事务相关的所有lock
        public synchronized void removeTidLocks(TransactionId tid) {
            for (Map.Entry<PageId, Vector<Lock>> entry : this.lockMap.entrySet()) {
                Vector<Lock> lockList = entry.getValue();
                for (int i = 0; i < lockList.size(); i++) {
                    if (lockList.get(i).getTid().equals(tid)) {
                        lockList.remove(lockList.get(i));
                    }
                }
                if (lockList.size() == 0) {
                    this.lockMap.remove(entry.getKey());
                }
            }
        }

        // 加锁
        public synchronized boolean setLock(PageId pageId, TransactionId tid, Permissions type) {
            Vector<Lock> curLockVector = this.lockMap.get(pageId);
            if (curLockVector == null) {  // 如果这个page没有上锁
                curLockVector = new Vector<>();
                curLockVector.add(new Lock(tid, type));
                this.lockMap.put(pageId, curLockVector);
                return true;
            } else {
                if (type.equals(Permissions.READ_WRITE)) {
                    // 申请独占锁
                    if (curLockVector.size() == 1) {
                        if (curLockVector.get(0).getTid().equals(tid)) {
                            // 只有一个锁并且锁的事务id一致，看是否升级锁
                            if (curLockVector.get(0).getPermission().equals(Permissions.READ_ONLY)) {
                                curLockVector.get(0).setPermission(type);
                                this.lockMap.put(pageId, curLockVector);
                            }
                            return true;
                        } else { // 当前页面已经被其他事务共享，暂时不能独占
                            return false;
                        }
                    } else { // 当前页面的锁有多个
                        return false;
                    }
                } else {
                    // 申请共享锁
                    for (Lock curLock : curLockVector) {
                        if (curLock.getPermission().equals(Permissions.READ_WRITE)) { // 如果这个锁是独占锁，此时只能有一个锁
                            return curLock.getTid().equals(tid) && curLockVector.size() == 1;
                        }
                        if (curLock.getTid().equals(tid)) {
                            return true;
                        }
                    }
                    curLockVector.add(new Lock(tid, type));
                    this.lockMap.put(pageId, curLockVector);
                    return true;
                }
            }
        }

        // 查看一个tid是否对pid加了lock
        public synchronized boolean holdLock(PageId pid, TransactionId tid) {
            Vector<Lock> curLockVector = this.lockMap.get(pid);
            for (Lock curLock : curLockVector) {
                if (curLock.getTid().equals(tid)) {
                    return true;
                }
            }
            return false;
        }

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

        public void setData(T data) {
            this.data = data;
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

    public void put(Node<Page> curNode) {
        // 向链表中添加
        curNode.setNext(this.head.getNext());
        curNode.setPrev(this.head);  // fix bug in lab3 exercise2
        this.head.getNext().setPrev(curNode);  // 不能忽略这一步
        this.head.setNext(curNode);
    }

    public void remove(Node<Page> curNode) {
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        boolean hasLock = false;
        long startTime = System.currentTimeMillis();
        long timeout = 100;
        while (!hasLock) {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new TransactionAbortedException();
            }
            hasLock = this.lockManager.setLock(pid, tid, perm);
        }

        Node<Page> curNode = this.lruCache.get(pid);
        if (curNode == null) {
            if (this.lruCache.size() == this.numPages) {
                evictPage();
            }
            curNode = new Node<>(Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            this.put(curNode);
            this.lruCache.put(pid, curNode);
            // 从磁盘读出page（page有自己的table id，而table和dbfile一一对应，dbfile是与磁盘交互的接口）
            // 将载入buffer pool的page给到curPage
        } else {
            this.remove(curNode);
            this.put(curNode);
        }
        return curNode.getData();
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
        this.lockManager.removeLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdLock(p, tid);
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
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            this.restorePages(tid);
        }
        lockManager.removeTidLocks(tid);
    }

    // 将脏页修改回磁盘的内容
    public synchronized void restorePages(TransactionId tid) {
        for (Node<Page> node : this.lruCache.values()) {
            if (tid.equals(node.getData().isDirty())) {  // 不能写反，可能isDirty()返回null
                Page originPage = Database.getCatalog().getDatabaseFile(
                        node.getData().getId().getTableId()).readPage(node.getData().getId()
                );  // 此时originPage的dirty标志为False
                this.remove(node);
                node.setData(originPage);
                this.put(node);
                this.lruCache.put(originPage.getId(), node);
            }
        }
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
            Node<Page> curNode = new Node<>(page);
            if (this.lruCache.get(page.getId()) != null) {
                // BTree的测试中，page可能会分裂，新分裂出来的page可能不在cache中，自然不用remove
                this.remove(this.lruCache.get(page.getId()));
            }
            this.put(curNode);
            this.lruCache.put(curNode.getData().getId(), curNode);
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
            Node<Page> curNode = new Node<>(page);
            if (this.lruCache.get(page.getId()) != null) {
                this.remove(this.lruCache.get(page.getId()));
            }
            this.put(curNode);
            this.lruCache.put(page.getId(), curNode);
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
        if (this.lruCache.get(pid) != null) {
            this.remove(this.lruCache.get(pid));
        }
        this.lruCache.remove(pid);
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
        TransactionId dirtier = dirtyPage.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, dirtyPage.getBeforeImage(), dirtyPage);
            Database.getLogFile().force();
        }
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(dirtyPage);  // 写入磁盘
        dirtyPage.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Node<Page> node = this.head.getNext();
        while (!node.equals(this.tail)) {
            node.getData().setBeforeImage();
            if (node.getData().isDirty() == tid) {
                this.flushPage(node.getData().getId());
            }
            node = node.getNext();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Node<Page> node = this.tail.getPrev();
        while (!node.equals(this.head) && node.getData().isDirty() != null) {
            node = node.getPrev();
        }
        if (node.equals(this.head)) { // 全部都是脏页
            throw new DbException("all pages are dirty");
        } else {
            this.discardPage(node.getData().getId());
        }
    }

}
