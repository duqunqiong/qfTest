package zookeeper.observer;

//定义一个电视观察者，也要接受到统一的消息，故必须要实现观察者接口

public class TVFollower implements Follower {
    @Override
    public void process(String event) {
        System.out.println("我是TV观察者，我接收到的消息如下：《"+event+"》，并进行自己的业务处理");
    }
}
