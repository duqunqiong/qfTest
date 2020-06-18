package mapreduce.order;

import org.apache.avro.Schema;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Order implements WritableComparable<Order> {

    private String orderId;
    private String proId;
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProId() {
        return proId;
    }

    public void setProId(String proId) {
        this.proId = proId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(orderId, order.orderId) &&
                Objects.equals(proId, order.proId) &&
                Objects.equals(price, order.price);
    }

    @Override
    public String toString() {
        return orderId + "\t" +
                proId + "\t" +
                price;
    }


    @Override
    public int hashCode() {
        int result = orderId != null ? orderId.hashCode() : 0;
        result = 31 * result + (proId != null ? proId.hashCode() : 0);
        result = 31 * result + (price != null ? price.hashCode() : 0);
        return result;
    }

    public int compareTo(Order o) {

        int result= orderId.compareTo(o.getOrderId());
        if (result == 0) {
            //如果当前记录中order相同,那么就用价格排序
            result = Double.compare(this.price, o.getPrice());
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(proId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        orderId=in.readUTF();
        proId=in.readUTF();
        price = in.readDouble();
    }


}
