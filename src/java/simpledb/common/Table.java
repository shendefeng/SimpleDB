package simpledb.common;

import simpledb.storage.DbFile;

/**
 * @author: yolopluto
 * @Date: created in 2024/2/6 20:35
 * @description: 目录的表
 * @Modified By:
 */
public class Table {
    /**
     * the contents of the table to add;  file.getId() is the identfier of
     * this file/tupledesc param for the calls getTupleDesc and getFile
     */
    private DbFile file;

    /**
     * the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     */
    private String name;

    /**
     * the name of the primary key field
     */
    private String pkeyField;

    public Table(DbFile file,String name,String pkeyField){
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public Table(DbFile file,String name){
        new Table(file,name,"");
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

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
}
