package hive.homework19;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class MovieRateUDF extends UDF {
    public String evaluate(String json) throws IOException {
        if (StringUtils.isEmpty(json)) {
            return null;
        }

        //使用obiectMapper可以把字符串直接转换为Bean对象
        //使用泛型，就不用类型强转
        ObjectMapper objectMapper = new ObjectMapper();
        MovieRateBean movieRateBean = objectMapper.readValue(json, MovieRateBean.class);
        String s = movieRateBean.toString();
        return s;

    }
}

