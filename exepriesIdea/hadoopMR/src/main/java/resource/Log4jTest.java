package resource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4jTest {
    // 定义log日志对象，因为默认是log4j作为日志类实现，
    // 但是为了扩展性，定义使用SLF4j来定义这个对象
    private Logger logger = LoggerFactory.getLogger(Log4jTest.class);

    @Test
    public void testLevel() {
        // 演示通过 logger 进行不同级别的输出
        logger.trace("trace ------ 基本跟踪信息输出 ------");
        logger.debug("debug -------- 调试信息输出 --------");
        logger.info("info ---------- 日常信息输出 --------");
        logger.warn("warn --------- 警告信息输出 ---------");
        logger.error("error ------ 错误信息输出 ----------");

    }
}
