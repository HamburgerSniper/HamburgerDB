package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    // key：页id
    // value：作用于该页的所有lock
    private Map<Integer, List<Lock>> lockCache;

    public LockManager() {
        this.lockCache = new ConcurrentHashMap<>();
    }

    /**
     * 获取锁
     *
     * @param tid
     * @param pageId
     * @param permissions
     * @return
     */
    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions) {
        Lock lock = new Lock(tid, permissions);
        int pid = pageId.getPageNumber();
        // 使用List实现的一个锁的阻塞队列(等待)
        List<Lock> locks = lockCache.get(pid);
        if (locks == null || locks.size() == 0) {
            locks = new ArrayList<>();
            locks.add(lock);
            lockCache.put(pid, locks);
            return true;
        }
        // 当只有一个事务抢占锁
        if (locks.size() == 1) {
            Lock curLock = locks.get(0);
            if (curLock.getTransactionId().equals(tid)) {
                // 判断是否对锁进行升级 --> 当只有一个事务t拥有一个对象的共享锁o，那么t可以自动将锁升级成一个排他锁
                if (curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_WRITE)) {
                    // 将锁的级别升级为READ_WRITE
                    curLock.setPermissions(Permissions.READ_WRITE);
                }
                return true;
            } else {
                if (curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_ONLY)) {
                    //  均为共享锁时候，将该lock加入阻塞队列
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }

        //当有多个事务抢占锁，说明必然是多个读事务
        if (lock.getPermissions().equals(Permissions.READ_WRITE)) {
            return false;
        }

        //每一个事物读锁并不需要重复获取
        for (Lock l : locks) {
            if (l.getTransactionId().equals(lock.getTransactionId())) {
                return true;
            }
        }
        locks.add(lock);
        return true;
    }


    /**
     * 释放锁
     *
     * @param tid
     * @param pageId
     */
    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        // 找到当前页的号编号
        int pid = pageId.getPageNumber();
        // 通过lockCache找到当前页的事务锁
        List<Lock> locks = lockCache.get(pid);
        for (Lock l : locks) {
            // 对于每一个属于tid事务的锁，进行释放
            if (l.getTransactionId().equals(tid)) {
                locks.remove(l);
                if (locks.size() == 0) {
                    lockCache.remove(pid);
                }
                return;
            }
        }
    }

    /**
     * 释放当前事务的所有锁
     *
     * @param tid
     */
    public synchronized void releaseAllLock(TransactionId tid) {
        for (Integer pid : lockCache.keySet()) {
            List<Lock> locks = lockCache.get(pid);
            for (Lock lock : locks) {
                if (lock.getTransactionId().equals(tid)) {
                    locks.remove(lock);
                    if (locks.size() == 0) {
                        lockCache.remove(pid);
                    }
                    break;
                }
            }
        }
    }

    /**
     * 判断是否持有锁
     *
     * @param tid
     * @param pageId
     * @return
     */
    public synchronized Boolean holdsLock(TransactionId tid, PageId pageId) {
        for (Lock lock : lockCache.get(pageId.getPageNumber())) {
            if (lock.getTransactionId().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
