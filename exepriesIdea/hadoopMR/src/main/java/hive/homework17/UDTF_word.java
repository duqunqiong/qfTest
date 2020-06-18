package hive.homework17;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class UDTF_word extends GenericUDTF {
    //定义一个要输出的字段的名称和类型
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        // 用 集合list来表示输出的列名
        ArrayList<String> outputKeyNames = new ArrayList<>();
        outputKeyNames.add("value");

        //定义要输出的类型 list,列的类型
        ArrayList<ObjectInspector> outputValueName = new ArrayList<>();
        outputValueName.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(outputKeyNames,outputValueName);

        //throw new IllegalStateException("Should not be called directly");
    }

    //process方法用来处理输入的每行数据，每行数据处理都要调用一次process， 类似与Mapper中 的map
    public void process(Object[] var1) throws HiveException {
        //得到一个参数，转化为字符串  类似："3,a,b,c,d"
        String inputString = var1[0].toString();

        //切分为数组
        String[] split = inputString.split(",");

        //对数组进行处理
        for (int i = 1; i <( split.length-1) ; i++) {
            String s = "";
            for (int j = 1; j <=3; j++){
                s+=split[i];
            }
            forward(s);
        }
    }

    public void close() throws HiveException { }


}
