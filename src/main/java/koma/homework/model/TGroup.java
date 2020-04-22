package koma.homework.model;

import java.util.List;

public class TGroup {

    private String taskId;
    private Integer tGroupId;
    private List<Integer> levels;
    private List<TUnit> tUnits;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Integer gettGroupId() {
        return tGroupId;
    }

    public void settGroupId(Integer tGroupId) {
        this.tGroupId = tGroupId;
    }

    public List<Integer> getLevels() {
        return levels;
    }

    public void setLevels(List<Integer> levels) {
        this.levels = levels;
    }

    public List<TUnit> gettUnits() {
        return tUnits;
    }

    public void settUnits(List<TUnit> tUnits) {
        this.tUnits = tUnits;
    }
}
