package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter 是SQL语句中where的基础，起到条件过滤的作用
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    /**
     * predicate 断言，实现条件过滤的重要属性
     */
    private Predicate predicate;
    /**
     * child 数据源，我们从这里获取一条一条的Tuple用predicate去过滤
     */
    private OpIterator child;

    private Iterator<Tuple> tupleIterator;
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
        return this.child.getTupleDesc();
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
        tupleIterator = childTuple.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        child.close();
        tupleIterator = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        tupleIterator = childTuple.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * <p>
     * 从child那里读取一个个tuple并进行判断，直到找到一个合适的可以返回的元组才能返回，如果child的遍历已经结束了，那么就返回一个null
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (tupleIterator != null && tupleIterator.hasNext()) {
            return tupleIterator.next();
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
