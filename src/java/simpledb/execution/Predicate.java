package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * field 代表tuple比较的第几个字段
     */
    private int field;
    /**
     * op 代表具体的运算符，包括：相等、大于、小于、等于、不等于、大于等于、小于等于、模糊查询这几种
     */
    private Op op;
    /**
     * operand是用于参与比较的，比如上述SQL语句select * from students where id > 2；
     * 假如id是第0个字段，这里的field = 0，op = GREATER_THAN（大于），operand = new IntField(1)。
     * 比较过滤的方法是现在 filter() 方法中
     */
    private Field operand;


    /**
     * 枚举运算符类
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    /**
     * Constructor.
     *
     * @param field   field number of passed in tuples to compare against.
     * @param op      operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        // some code goes here
        return op;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        // some code goes here
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * Predicate的作用就是将传入的Tuple进行判断，而Predicate的field属性表明使用元组的第几个字段去
     * 与操作数operand进行op运算操作，比较的结果实际是调用Field类的compare方法，compare方法会根据
     * 传入的运算符和操作数进行比较
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        Field field = t.getField(this.field);
        return field.compare(this.op,this.operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    @Override
    public String toString() {
        return "Predicate{" +
                "field=" + field +
                ", op=" + op +
                ", operand=" + operand +
                '}';
    }
}
