import org.junit.Test;

/**
 * @ClassName MapReduceDemo-DemoTes
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月18日16:10 - 周六
 * @Describe
 */
public class DemoTest {
    @Test
    public void show() throws InterruptedException {
        dfs(10);
    }

    public void dfs(int num) {
        if (num == 0)
            return;
        for (int i = 0; i < num; i++) {
            if (i == num / 2) {
                System.out.println(i);
                dfs(num / 2);
            }
        }
    }
}
