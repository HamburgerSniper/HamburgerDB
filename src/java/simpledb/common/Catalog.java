package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 日志，直接对应持久层
 * <p>
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
    private final Map<Integer, Table> tableMap;

    /**
     * Table：为Catalog存储的一个个表建立的辅助类，Table类的构造函数需要三个参数，
     * 第一个参数是DbFile类型，是table的内容；
     * 第二个参数是String类型，是table 的name；
     * 第三个参数是pkeyField，代表表中主键的fieldName。
     */
    public class Table {
        private DbFile file;

        private String name;
        private String pKeyName;

        public Table(DbFile file, String name, String pKeyName) {
            this.file = file;
            this.name = name;
            this.pKeyName = pKeyName;
        }

        public DbFile getFile() {
            return file;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getpKeyName() {
            return pKeyName;
        }

        public void setpKeyName(String pKeyName) {
            this.pKeyName = pKeyName;
        }

        @Override
        public String toString() {
            return "Table{" +
                    "DbFile=" + file +
                    ", name='" + name + '\'' +
                    ", pKeyName='" + pKeyName + '\'' +
                    '}';
        }
    }

    /**
     * 创建一个<Interger,Table>的哈希表，用于存储已经实例化的表。
     * <p>
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this.tableMap = new HashMap<>();
    }

    /**
     * 在哈希表中添加一个Table
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table table = new Table(file, name, pkeyField);
        for (int i = 0; i < this.tableMap.size(); i++) {
            Table tmp = this.tableMap.get(i);
            if (tmp.getName() == null) {
                continue;
            }
            if (tmp.getName().equals(name) || tmp.getFile().getId() == file.getId()) {
                this.tableMap.put(i, table);
                return;
            }
        }
        this.tableMap.put(this.tableMap.size(), table);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (name != null) {
            for (int i = 0; i < this.tableMap.size(); i++) {
                if (this.tableMap.get(i).getName() == null) {
                    continue;
                }
                if (this.tableMap.get(i).getName().equals(name)) {
                    return this.tableMap.get(i).getFile().getId();
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 根据TableId获取Table
     *
     * @param tableId
     * @return
     */
    public Table getTableById(int tableId) {
        for (int i = 0; i < this.tableMap.size(); i++) {
            Table table = this.tableMap.get(i);
            if (table.getFile().getId() == tableId) {
                return table;
            }

        }
        return null;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        // some code goes here
        Table table = getTableById(tableId);
        if (table != null) {
            return table.getFile().getTupleDesc();
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
        // some code goes here
        Table table = getTableById(tableId);
        if (table != null) {
            return table.getFile();
        }
        throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableId) {
        // some code goes here
        Table table = getTableById(tableId);
        if (table != null) {
            return table.getpKeyName();
        }
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < this.tableMap.size(); i++) {
            result.add(this.tableMap.get(i).getFile().getId());
        }
        return result.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table table = getTableById(id);
        if (table != null) {
            return table.getName();
        }
        throw new NoSuchElementException();
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        this.tableMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

