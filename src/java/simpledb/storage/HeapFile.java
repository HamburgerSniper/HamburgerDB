package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * 返回磁盘中支持此HeapFile 的File类型文件。
     * <p>
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * 返回唯一标识此HeapFile的ID
     * <p>
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * 返回存储在这个DbFile中的table 的TupleDesc。
     * <p>
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /*
        以下有些方法并不会直接调用，而是通过BufferPool调用
     */
    // see DbFile.java for javadocs

    /**
     * 根据给定页号来读取 File 中对应的 Page
     * 通过Java提供的文件随机访问API实现
     * <p>
     * 读取pid对应的Page。先找到File内要读取的Page Number(startpositon)，读取整个Page返回。
     * <p>
     * 随机访问就是可以从文件的任何一个字节开始向下读取，而顺序访问就是必须从文件头开始读
     *
     * @param pid
     * @return
     */
    public Page readPage(PageId pid) {
        // some code goes here
        byte data[] = new byte[BufferPool.getPageSize()];
        int startPosition = pid.getPageNumber() * BufferPool.getPageSize();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            randomAccessFile.seek(startPosition);
            randomAccessFile.read(data, 0, data.length);
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // see DbFile.java for javadocs

    /**
     * 写pid对应的Page。先找到File内要写的Page Number，写入整个Page。
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId heapPageId = (HeapPageId) page.getId();
        int size = BufferPool.getPageSize();
        int pageNumber = heapPageId.getPageNumber();
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pageNumber* size);
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * 返回这个HeapFile中包含的page数量。
     * <p>
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs

    /**
     * 找到一个未满的page，如果不存在空闲的slot，创建新的一页存储tuple，之后添加，返回添加过的Page。
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!getFile().canRead() || !getFile().canWrite()) {
            throw new IOException();
        }
        List<Page> res = new ArrayList<>();
        for(int i=0;i<numPages();i++){
            HeapPageId heapPageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
            if(heapPage==null){
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
            if(heapPage.getNumEmptySlots()==0){
                Database.getBufferPool().unsafeReleasePage(tid,heapPageId);
                continue;
            }
            heapPage.insertTuple(t);
            heapPage.markDirty(true,tid);
            res.add(heapPage);
            return res;
        }
        //新建一个page
        HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        res.add(heapPage);
        return res;

    }

    // see DbFile.java for javadocs

    /**
     * 找到对应的page，删除tuple，标识此page为dirty。
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to delete.  This tuple should be updated to reflect that
     *            it is no longer stored on any page.
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<>();
        HeapPageId heapPageId  = (HeapPageId) t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        if(heapPage==null){
            throw  new DbException("null");
        }
        heapPage.deleteTuple(t);
        res.add(heapPage);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid,Permissions.READ_ONLY);
    }

    /**
     * 这个迭代器的作用是用来遍历所有的tuple，但是不要将所有tuple一次性放入内存，而是一页一页的读和遍历
     */
    public class HeapFileIterator implements DbFileIterator{
        TransactionId tid;
        Permissions permissions;
        BufferPool bufferPool =Database.getBufferPool();
        /**
         * iterator 每一页的迭代器
         */
        Iterator<Tuple> iterator;
        int num = 0;

        public HeapFileIterator(TransactionId tid,Permissions permissions){
            this.tid = tid;
            this.permissions = permissions;
        }

        /**
         * 开始进行遍历，默认从第一页开始
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            HeapPageId heapPageId = new HeapPageId(getId(), num);
            HeapPage page = (HeapPage)this.bufferPool.getPage(tid, heapPageId, permissions);
            if(page==null){
                throw  new DbException("page null");
            }else{
                iterator = page.iterator();
            }
        }

        /**
         * 获取下一有数据的页
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public boolean nextPage() throws DbException, TransactionAbortedException {
            while(true){
                num++;
                if(num>=numPages()){
                    return false;
                }
                HeapPageId heapPageId = new HeapPageId(getId(), num);
                HeapPage page = (HeapPage)bufferPool.getPage(tid,heapPageId,permissions);
                if(page==null){
                    continue;
                }
                iterator = page.iterator();
                if(iterator.hasNext()){
                    return true;
                }
            }
        }



        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(iterator==null){
                return false;
            }
            if(iterator.hasNext()){
                return true;
            }else{
                return nextPage();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(iterator==null){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

