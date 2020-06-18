package zookeeper.observer;

//进行观察者模式的测试

public class Client {
    public static void main(String[] args){
        // 创建一个subject（主导者）
        Leader subject=new Leader();

        //创建多个跟随者，父类声明子类实现，提高扩展性
        Follower netFollower=new NetFollower();
        Follower phoneFollower=new PhoneFollower();
        Follower TVFollower=new TVFollower();

        //把多方observer注册到一方subject
        subject.addObserver(netFollower);
        subject.addObserver(phoneFollower);
        subject.addObserver(TVFollower);

        //当主导方的subject发生变化时候，通知多方
        subject.notifyAllObject("服务器上线了");

    }
}
