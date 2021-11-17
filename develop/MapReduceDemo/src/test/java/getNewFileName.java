import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName MapReduceDemo-getFileName
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月10日10:27 - 周五
 * @Theme output1
 * output10
 * output2
 * output3
 */
public class getNewFileName {
    @Test
    public void test01() {
        File dir = new File("T:\\ShangGuiGu\\hadoop\\output"); //获取当前目录下的文件以及文件夹的名称。
        String[] names = dir.list();
        int lastNum = 0;
        for (String name : names) {
            int temp = 0;
            String[] ts = name.split("put");
            if ((temp = Integer.parseInt(ts[1])) > lastNum) {
                lastNum = temp;
            }
        }
        System.out.println("T:\\User\\Desktop\\output\\output" + (lastNum + 1));
    }

    @Test
    public void test02() throws Exception {
        Properties info = new Properties();
        String filePath = "src\\main\\resources\\filenum.properties";
        info.load(new FileInputStream(filePath));

        int nowFileName = Integer.parseInt(info.getProperty("lastNum"));

        info.setProperty("lastNum", String.valueOf(nowFileName + 1));
        info.store(new FileOutputStream(filePath), null);
        System.out.println("T:\\User\\Desktop\\output\\output" + nowFileName);
    }

    @Test
    public void test03(){
        List<?> list = new ArrayList<>();
        list.add(null);

    }

}
