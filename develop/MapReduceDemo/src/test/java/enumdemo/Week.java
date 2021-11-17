package enumdemo;

/**
 * @ClassName MapReduceDemo-Week
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月12日15:09 - 周日
 * @Theme
 */
public enum Week {
    MONDAY("-monday"),
    TUESDAY("-tuesday"),
    WEDNESDAY("-wednesday"),
    THURSDAY("-thursday"),
    FRIDAY("-friday"),
    SUNDAY("-sunday");

    private String weekName;

    private Week(String value){
        this.weekName = value;
    }

    public String getWeekName(){
        System.out.println(weekName);
        return weekName;
    }
}
