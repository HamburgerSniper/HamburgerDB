package simpledb.transaction;

import simpledb.common.Permissions;

public class Lock {
    private final TransactionId transactionId;
    private Permissions permissions;

    public Lock(TransactionId transactionId, Permissions permissions) {
        this.transactionId = transactionId;
        this.permissions = permissions;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public Permissions getPermissions() {
        return permissions;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        return "Lock{" +
                "permissions=" + permissions +
                ", transactionId=" + transactionId +
                '}';
    }
}
