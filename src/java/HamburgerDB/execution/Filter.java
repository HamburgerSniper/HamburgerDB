package HamburgerDB.execution;

import HamburgerDB.common.DbException;
import HamburgerDB.storage.Tuple;
import HamburgerDB.storage.TupleDesc;
import HamburgerDB.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator child;
    private TupleDesc tupleDesc;
    private Iterator<Tuple> it;
    private final List<Tuple> childTuple = new ArrayList<>();

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            if (predicate.filter(next)) {
                childTuple.add(next);
            }
        }
        it = childTuple.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = childTuple.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
