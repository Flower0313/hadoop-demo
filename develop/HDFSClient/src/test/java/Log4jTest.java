import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * @ClassName HDFSClient-Log4jTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日18:22 - 周日
 * @Theme
 */
public class Log4jTest {

    /**
     * 配置文件中定义的日志级别是info，所以只会执行比他低的
     */
    @Test
    public void test01(){
        //申明Logger，记得别导错包
        Logger logger = Logger.getLogger(Log4jTest.class);
        logger.debug("这是debug");
        logger.info("这是info");
        logger.warn("这是warn");
        logger.error("这是error");
        logger.fatal("这是fatal");
    }
}
