package HamburgerDB.common;

import HamburgerDB.storage.BufferPool;
import HamburgerDB.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database is a class that initializes several static variables used by the
 * database system (the catalog, the buffer pool, and the log files, in
 * particular.)
 * <p>
 * Provides a set of methods that can be used to access these variables from
 * anywhere.
 *
 * @Threadsafe
 */
public class Database {
    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    private final Catalog _catalog;
    private final BufferPool _bufferpool;

    private final static String LOGFILENAME = "log";
    private final LogFile _logfile;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
        // startControllerThread();
    }

    /**
     * Return the log file of the static Database instance
     * 返回静态数据库实例的日志文件
     */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /**
     * Return the buffer pool of the static Database instance
     * 返回静态数据库实例的缓冲池
     */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    /**
     * Return the catalog of the static Database instance
     * 返回静态数据库实例的目录
     */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    /**
     * Method used for testing -- create a new instance of the buffer pool and
     * return it
     * 用于测试的方法——创建缓冲池的新实例并返回它
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF = null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
//        _instance._bufferpool = new BufferPool(pages);
        return _instance.get()._bufferpool;
    }

    // reset the database, used for unit tests only.
    // 重置数据库，仅用于单元测试。
    public static void reset() {
        _instance.set(new Database());
    }

}
