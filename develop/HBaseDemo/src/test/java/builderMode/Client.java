package builderMode;

/**
 * @ClassName MapReduceDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月10日18:26 - 周日
 * @Describe
 */
public class Client {
    public static void main(String[] args) {
        OuterBuilder flower = OuterBuilder.newBuilder("flower");
        flower.setAge(22);
        System.out.println(flower.toString());
    }
}
