package HamburgerDB.common;

import HamburgerDB.storage.StringField;
import HamburgerDB.storage.Field;
import HamburgerDB.storage.IntField;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 * SimpleDB 中的类型：类型是由这个类定义的静态对象;因此，Type构造函数是私有的。
 */
public enum Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte[] bs = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN - strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    };

    public static final int STRING_LEN = 128;

    /**
     * @return the number of bytes required to store a field of this type.
     */
    public abstract int getLen();

    /**
     * @param dis The input stream to read from
     * @return a Field object of the same type as this object that has contents
     * read from the specified DataInputStream.
     * 返回与此对象相同类型的Field对象，其内容从指定的DataInputStream中读取。
     * @throws ParseException if the data read from the input stream is not
     *                        of the appropriate type.
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
