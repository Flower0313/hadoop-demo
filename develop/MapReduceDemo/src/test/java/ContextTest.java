import sun.plugin.services.WPlatformService;

/**
 * @ClassName MapReduceDemo-ContextTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月11日14:07 - 周六
 * @Theme
 */
public class ContextTest {
    public static void main(String[] args) {
    }
}

interface Human{
    public void iam();
}

class Man implements Human{
    @Override
    public void iam() {
        System.out.println("我是男人");
    }
}

class SmallMan extends Man{

}
interface Woman extends Human{

}









