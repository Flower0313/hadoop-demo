
import org.apache.flume.FlumeException;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MapReduceDemo-Event
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月13日10:51 - 周三
 * @Describe
 */
public class Event{
    private Map<String, String> headers = new HashMap<>();
    private String body;

    public Map<String, String> getHeaders() {
        headers.put("1","xiaohua");
        return headers;
    }

    public Map<String,String> getMap(){
        return headers;
    }

}
