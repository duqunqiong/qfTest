package zookeeper.observer;

//定义一个网络观察者，因为所有观察者都要统一接收消息，观察者子类必须实现观察者接口

public class NetFollower implements Follower {
    //process 用来处理当网络观察者接收主导者subject信息，进行业务处理，展示例子：打印

    @Override
    public void process(String event) {
        System.out.println("我是网络观察者，收到如下信息：<"+event+">进行自己得业务处理");
    }
}
