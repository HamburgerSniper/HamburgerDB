package HamburgerDB.execution;

import HamburgerDB.common.Database;
import HamburgerDB.storage.DbFile;
import HamburgerDB.transaction.TransactionAbortedException;
import HamburgerDB.transaction.TransactionId;
import HamburgerDB.common.Type;
import HamburgerDB.common.DbException;
import HamburgerDB.storage.DbFileIterator;
import HamburgerDB.storage.Tuple;
import HamburgerDB.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;
    private String tableAlias;
    private DbFileIterator dbFileIterator;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
        this.transactionId = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;

    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        /* 自加 some codes here */
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableId    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableId, String tableAlias) {
        // some code goes here
        this.tableId = tableId;
        this.tableAlias = tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDatabaseFile(this.tableId);
        DbFileIterator iterator = dbFile.iterator(this.transactionId);
        this.dbFileIterator = iterator;
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc();
        int itemLen = tupleDesc.numFields();
        Type[] types = new Type[itemLen];
        String[] fieldNames = new String[itemLen];
        for (int i = 0; i < itemLen; i++) {
            types[i] = tupleDesc.getFieldType(i);
            fieldNames[i] = this.tableAlias + "." + tupleDesc.getFieldName(i);
        }

        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.dbFileIterator.next();
    }

    public void close() {
        // some code goes here
        this.dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.dbFileIterator.rewind();
    }
}
