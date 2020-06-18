package zookeeper.observer;

import java.util.ArrayList;
import java.util.List;

public class Leader  {
    // 定义一个一对多的关系列表：一个主导者对应多个跟随者
    private List<Follower> list = new ArrayList<>();

    // 更随者可以动态的注册到我们的主导者
    public void addObserver(Follower follower){
        list.add(follower);
    }

    //主导者发生变化时，通知到各个跟随者
    public void notifyAllObject(String event) {
        for (Follower follower:list) {
            follower.process(event);
        }
    }
}
