package zookeeper.observer;

// 定义观察者模式的跟随者

//主导者发生变化时，调用一个方法来通知跟随者
public interface Follower {
    void process(String event);
}
