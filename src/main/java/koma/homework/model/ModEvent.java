package koma.homework.model;

public class ModEvent {

    public static final String OP_CREATE = "c";
    public static final String OP_UPDATE = "u";
    public static final String OP_DELETE = "d";

    private String op;
    private String after;
    private String patch;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getAfter() {
        return after;
    }

    public void setAfter(String after) {
        this.after = after;
    }

    public String getPatch() {
        return patch;
    }

    public void setPatch(String patch) {
        this.patch = patch;
    }
}
