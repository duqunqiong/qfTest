package mapreduce.cattest;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个Hadoop的序列化的类,要实现Writable接口
 * 因为在mr中的key必须要可以排序,如果我们用自定义的对象做key,
 * 那么这个对象一定要实现WritableComparable接口,并且实现其中的方法
 */
public class Cat implements WritableComparable<Cat> {
    private  String name;
    private  int age;
    private  int price;
    private  String number;

    public Cat() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cat cat = (Cat) o;

        if (age != cat.age) return false;
        if (price != cat.price) return false;
        if (name != null ? !name.equals(cat.name) : cat.name != null) return false;
        return number != null ? number.equals(cat.number) : cat.number == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + age;
        result = 31 * result + price;
        result = 31 * result + (number != null ? number.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return name+"\t"+age+"\t"+price+"\t"+number;
    }

    /**
     * write是用来持久化中写数据,注意要用传入的参数对象out进行持久化
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        //如果字段类型是String,那么要调用的方法是writeUTF
        out.writeUTF(name);
        out.writeInt(age);
        out.writeInt(price);
        out.writeUTF(number);

    }

    /**
     * readFields用来做序列化中读取对象的值,使用传参对象in来进行读取,并且读取后要给当前字段赋值
     * 特别注意:字段的读写顺序必须一致:在write方法中写的顺序和readFields读的顺序必须一样
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        name=in.readUTF();
        age = in.readInt();
        price = in.readInt();
        number=in.readUTF();
    }

    @Override
    public int compareTo(Cat o) {
     //qqq 如果在排序条件中可以再次排序,那么这个排序也可以叫做二次排序
        int i = name.compareTo(o.getName());
        if (i==0) {
            i = Integer.compare(age, o.getAge());
        }
        return i;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }
}
