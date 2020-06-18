package hive.homework18;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;


//给当前函数添加一个描述信息，方便再desc function方法的时候查看
@Description(name = "maxInt",value = "Find max Value", extended = "Extended:Find Max Value for all Col")

public class MaxValue extends UDAF {
    public static class MaxnumInt implements UDAFEvaluator{
    //在静态内部类定义一个返回值，作为当前UDAF最后的唯一返回值，因为返回值要在hive中调用，所以必须要使用序列化的类型
        private IntWritable result;

        //初始化时候，返回值为null
        @Override
        public void init() {
            result = null;
        }

        public boolean iterate(IntWritable value){
            //用来遍历每行数据value值得传入，并比较大小
            if (value == null){return true;}

            //如果是第一行数据，那儿直接给result赋值为第一行数据
            if (result == null){
                result = new IntWritable(value.get());
            }else {
                //不是第一行得时候,比较两个值得最大值得
                result.set(Math.max(result.get(),value.get()));
            }

            return true;
        }

        //定义一个函数itreate 用来处理遍历多行时，每行值传进来是调用的函数
        //再map端进行并执行后得结果
        public IntWritable terminatePartial(){return result;}

        //接受terminatePartail的返回结果，进行数据的merge操作，其返回类型为boolean
        public boolean merge(IntWritable other){return iterate(other);}

        //将最终的结果返回为hive
        public IntWritable terminate(){return result;}

    }
}
