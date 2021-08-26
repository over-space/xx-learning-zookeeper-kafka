package com.learning.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient implements Watcher {

    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent event) {
        System.out.println("触发创建ZookeeperClient#Watcher事件，stage: " + event.getState());
        if(event.getState() == Event.KeeperState.SyncConnected){
            latch.countDown();
        }
    }

    public ZooKeeper connection(String host, int timeout){
        ZooKeeper zookeeper = null;
        try {
            zookeeper = new ZooKeeper(host, timeout, this);

            // 等待zookeeper连接成功
            latch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return zookeeper;
    }
}
