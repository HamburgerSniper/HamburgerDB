package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * pageId : 页号
     */
    private PageId pageId;
    /**
     * TupleNum : 页面偏移量
     */
    private Integer tupleNumber;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageId of the page on which the tuple resides
     * @param tupleNo the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNo) {
        // some code goes here
        this.pageId = pid;
        this.tupleNumber = tupleNo;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return this.tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        } else if (o instanceof RecordId) {
            RecordId temp = (RecordId) o;
            if (temp.tupleNumber.equals(this.tupleNumber) && temp.pageId.equals(this.pageId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        return tupleNumber.hashCode() + (pageId + "").hashCode();

    }

}
