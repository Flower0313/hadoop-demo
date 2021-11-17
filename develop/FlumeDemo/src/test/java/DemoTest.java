import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MapReduceDemo-DemoTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月28日11:42 - 周二
 * @Describe
 */
public class DemoTest {
    public static void main(String[] args) {
        /*String name = "flower";
        byte[] names = name.getBytes();
        names[0] ='x';

        String rename = new String(names, StandardCharsets.UTF_8);
        System.out.println(rename);*/
        Event event = new Event();
        intercept(event);
        showMap(event.getHeaders());
    }

    public static Event intercept(Event event){
        Map<String, String> headers = event.getHeaders();
        //打印出两个的哈希值能知道他们的地址是一样的
        System.out.println("event.headers"+event.getHeaders().hashCode());
        System.out.println("diy.headers"+headers.hashCode());
        headers.put("2","zzx");
        return event;
    }

    public static void showMap(Map<?,?> map){
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }
}
