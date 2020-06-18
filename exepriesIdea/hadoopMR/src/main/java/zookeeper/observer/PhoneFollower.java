package zookeeper.observer;

//定义一个手机观察者
//所有观察者都需要接收到统一的消息，所以观察者都需要实现观察者接口

public class PhoneFollower implements Follower {
    //process处理接受到的消息，并进行处理，示例：打印该消息

    @Override
    public void process(String event) {
        System.out.println("我是手机观察者，接受到的消息是：《"+event+"》，并进行自己的业务处理");
    }

}
