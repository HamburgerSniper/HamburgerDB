package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * <b> 元组 </b>
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    /**
     * 验证版本一致性的serialVersionUID
     */
    private static final long serialVersionUID = 1L;
    /**
     * 元组头部
     */
    private TupleDesc tupleDesc;
    /**
     * 储存的数据
     */
    private List<Field> fieldList;
    /**
     * 路径
     */
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     * 使用指定的架构（类型）创建一个新的元组。
     * <p>
     * td: 元组的模式，必须是一个有效的 TupleDesc 实例 至少有一个字段
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.fieldList = new ArrayList<>(td.numFields());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= this.fieldList.size()) {
            this.fieldList.add(i, f);
        } else {
            this.fieldList.set(i, f);
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i > this.fieldList.size()) {
            return null;
        }
        return this.fieldList.get(i);
    }

    /**
     * 输出内容，因此并不需要返回RecordId路径，因此不需要RecordId字段
     * <p>
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    @Override
    public String toString() {
        return "Tuple{" +
                "tupleDesc=" + tupleDesc +
                ", fieldList=" + fieldList +
                '}';
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return this.fieldList.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Tuple) {
            if (((Tuple) o).tupleDesc.equals(tupleDesc) && ((Tuple) o).recordId.equals(recordId) && ((Tuple) o).fieldList.equals(fieldList)) {
                return true;
            }
        }
        return false;
    }

}
