package mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    public  OrderGroupingComparator(){
        super(Order.class,true);
    }

    public  int compare(WritableComparable a, WritableComparable b){
        Order orderBefore = (Order)a;
        Order orderAfter = (Order) b;
        return  orderAfter.getOrderId().compareTo(orderBefore.getOrderId());
    }
}
