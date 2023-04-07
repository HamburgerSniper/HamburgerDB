package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> tupleDescList = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * <br>
     * <b>帮助类，便于组织每个字段的信息</b>
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * <br>
         * <b>字段类型</b>
         */
        public final Type fieldType;

        /**
         * The name of the field
         * <br>
         * <b>字段名称</b>
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public Type getFieldType() {
            return fieldType;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }


    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     * <br>
     * <b>一个迭代器，用于迭代此TupleDesc中包含的所有字段TDItems</b>
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tupleDescList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     *                <b>指定此TupleDesc中字段的数量和类型，至少包含一个条目</b>
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     *                <b>指定字段名称的数组。请注意，名称可能为null</b>
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                this.tupleDescList.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for (int i = 0; i < typeAr.length; i++) {
            this.tupleDescList.add(new TDItem(typeAr[i], null));
        }
    }

    public TupleDesc(List<TDItem> tdItems) {
        this.tupleDescList = tdItems;
    }

    /**
     * 获取TDItem的数量
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tupleDescList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tupleDescList.size()) {
            throw new NoSuchElementException();
        }
        return this.tupleDescList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= tupleDescList.size()) {
            throw new NoSuchElementException();
        }
        return this.tupleDescList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        for (int i = 0; i < this.tupleDescList.size(); i++) {
            String fieldName = this.tupleDescList.get(i).getFieldName();
            if (fieldName == null) {
                continue;
            }
            if (fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 获取所有元数据的字节大小
     *
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int result = 0;
        for (int i = 0; i < this.tupleDescList.size(); i++) {
            result += this.tupleDescList.get(i).fieldType.getLen();
        }
        return result;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> tupleDescList01 = td1.tupleDescList;
        List<TDItem> tupleDescList02 = td2.tupleDescList;
        List<TDItem> tupleDescListResult = new ArrayList<>();
        tupleDescListResult.addAll(tupleDescList01);
        tupleDescListResult.addAll(tupleDescList02);
        return new TupleDesc(tupleDescListResult);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            List<TDItem> tupleDescList = ((TupleDesc) o).tupleDescList;
            if (tupleDescList.size() == this.tupleDescList.size()) {
                for (int i = 0; i < this.tupleDescList.size(); i++) {
                    if (!this.tupleDescList.get(i).getFieldType().equals(tupleDescList.get(i).getFieldType())) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }


    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        return "TupleDesc{" +
                "tupleDescList=" + tupleDescList +
                '}';
    }
}
