package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;  // 这个page存储的tuple
    final int numSlots;
    private TransactionId transactionId;  // 记录上一个修改这个Page的事务ID
    private boolean isDirty;  // 标记这个page是否被事务修改过

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        // page_size * 8.0 / (tuple_size*8+1)
        return (int) Math.floor(
                BufferPool.getPageSize() * 8.0 / (Database.getCatalog().getTupleDesc(pid.getTableId()).getSize() * 8 + 1)
        );
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(this.getNumTuples() / 8.0);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId().getTupleNumber() >= this.numSlots) {  // 判断tuple的tupleNo是否超过Page的限制
            throw new DbException("tuple is not on this page");
        }
        if (!this.td.equals(t.getTupleDesc())) {  // tupledesc不匹配，因此不在这个page上
            throw new DbException("tuple is not on this page");
        }
        if (tuples[t.getRecordId().getTupleNumber()] == null) {  // 判断该page的相应位置上有没有tuple
            throw new DbException("tuple is not on this page");
        }
        if (!isSlotUsed(t.getRecordId().getTupleNumber())) {  // 判断该slot是否为空
            throw new DbException("slot is empty");
        }
        if (!t.getRecordId().getPageId().equals(this.pid)) {  // 判断相应位置上的tuple是否相同
            throw new DbException("tuple is not on this page");
        }
        this.tuples[t.getRecordId().getTupleNumber()] = null;  // 删除该page存储的tuple
        this.markSlotUsed(t.getRecordId().getTupleNumber(), false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!this.td.equals(t.getTupleDesc())) {  // tupleDesc不一致
            throw new DbException("tuple should not be inserted in this page");
        }
        if (this.getNumEmptySlots() == 0) {  // Page满了
            throw new DbException("page is full");
        }
        // tuple还没有插入，因此不需要判断Pageid是否相同
        // 找一个空的slot，插入tuple
        for (int i = 0; i < this.numSlots; i++) {
            if (!this.isSlotUsed(i)) {
                this.tuples[i] = t;
                t.setRecordId(new RecordId(this.pid, i));
                markSlotUsed(i, true);
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.isDirty = dirty;
        if (dirty) {
            this.transactionId = tid;
        } else {
            this.transactionId = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        if (this.isDirty) {
            return this.transactionId;
        } else {
            return null;
        }
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int num = 0;
        for (int i = 0; i < this.numSlots; i++) {
            if (!this.isSlotUsed(i)) {
                num += 1;
            }
        }
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // 这是一个bitmap！每一个bit代表一个slot是否被占用
        int bytes = i / 8;
        int bits = i % 8;
        return ((header[bytes] >> bits) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int byteIndex = i / 8;
        int bitIndex = i % 8;
        if (value) {
            // 插入
            this.header[byteIndex] |= (1 << bitIndex);  // 改动这一bit的内容为1
        } else {
            // 删除
            this.header[byteIndex] &= ~(1 << bitIndex);  // 改动这一bit的内容为0
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        ArrayList<Tuple> tupleArrayList = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if (this.isSlotUsed(i)) {
                tupleArrayList.add(this.tuples[i]);
            }
        }
        return tupleArrayList.iterator();
    }

}

