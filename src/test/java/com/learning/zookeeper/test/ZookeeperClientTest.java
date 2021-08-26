package com.learning.zookeeper.test;

import com.learning.zookeeper.ZookeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ZookeeperClientTest {

    private static ZooKeeper zookeeper;

    @BeforeAll
    public static void before(){
        zookeeper = new ZookeeperClient().connection("192.168.181.210:2181/tms", 5000);
    }

    @Test
    public void testZookeeper(){
        System.out.println(zookeeper);
    }

    @Test
    public void testCreate(){
        try {
            zookeeper.create("/salary", "hello".getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
