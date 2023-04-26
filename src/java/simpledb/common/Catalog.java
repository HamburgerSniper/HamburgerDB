package simpledb.common;

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
 * Catalog类起到的作用相当于数据库的目录，记录了数据库中有哪些数据表，每个数据表对应的ID、数据文件、主键是什么东西，
 * 并定义了一系列增删查的方法，同时Database类中会有一个Database.getCatalog()的静态方法来访问整个数据库的Catalog
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
    private final ConcurrentHashMap<Integer, Table> tableMap;

    /**
     * Table：为Catalog存储的一个个表建立的辅助类，Table类的构造函数需要三个参数，
     * 第一个参数是DbFile类型，是table的内容；
     * 第二个参数是String类型，是table的name；
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
     * 创建一个<Integer,Table>的哈希表，用于存储已经实例化的表。
     * <p>
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this.tableMap = new ConcurrentHashMap<>();
    }

    /**
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
        this.tableMap.put(file.getId(), new Table(file, name, pkeyField));
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
        Integer res = tableMap.searchValues(1, value -> {
            if (value.getName().equals(name)) {
                return value.getFile().getId();
            }
            return null;
        });
        if (res != null) {
            return res;
        }
        throw new NoSuchElementException("not found id for table " + name);
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
        Table table = tableMap.getOrDefault(tableId, null);
        if (table != null) {
            return table.getFile().getTupleDesc();
        }
        throw new NoSuchElementException("not found tupleDesc for table" + tableId);
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
        Table table = this.tableMap.getOrDefault(tableId, null);
        if (table != null) {
            return table.getFile();
        }
        throw new NoSuchElementException("not found DbFile for table" + tableId);
    }

    public String getPrimaryKey(int tableId) {
        // some code goes here
        Table table = this.tableMap.getOrDefault(tableId, null);
        if (table != null) {
            return table.getpKeyName();
        }
        throw new NoSuchElementException("not found primaryKey for table" + tableId);
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
        Table table = this.tableMap.getOrDefault(id, null);
        if (table != null) {
            return table.getName();
        }
        throw new NoSuchElementException("not found tableName for table" + id);

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
        // 根目录
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            // 读取catalogFile
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

