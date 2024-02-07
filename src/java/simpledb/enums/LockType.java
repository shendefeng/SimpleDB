package simpledb.enums;

/**
 * @author: yolopluto
 * @Date: created in 2024/2/5 21:34
 * @description: 锁的类别
 * @Modified By:
 */
public enum LockType {
    SHARE_LOCK(0, "共享锁"),
    EXCLUSIVE_LOCK(1, "排它锁");
    private Integer code;
    private String value;

    LockType(int code, String value) {
        this.code = code;
        this.value = value;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
