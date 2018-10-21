package com.inca.zkserverlist;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Hello world!
 *
 */
public class ApplicationListener {
	private static CountDownLatch sampleLatch = new CountDownLatch(1);
	// zookeeper连接地址信息
	private static String connectString = "192.168.159.10,192.168.159.30,192.168.159.50";
	private static final String GROUPNODE = "/szw-servers";
	private static final String SUBNODE = GROUPNODE + "/server-";
	private static int sessionTimeout = 2000;
	private static ZooKeeper zk = null;
	private static Watcher watcher = new Watcher() {
		public void process(WatchedEvent event) {
			if (event.getState() == KeeperState.SyncConnected) {
				sampleLatch.countDown();/* ZK连接成功时，计数器由1减为0 */
			}
			System.out.println("事件:" + event);
			System.out.println("类型:" + event.getType());
			System.out.println("路径:" + event.getPath());
			System.out.println("状态:" + event.getState());
			try {
				zk.exists(GROUPNODE, true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	};

	public ApplicationListener() {
	}

	public void connectZkServer(String serverName) throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, watcher);
		if (zk.getState() == States.CONNECTING) {
			try {
				sampleLatch.await();
			} catch (InterruptedException err) {
				System.out.println("Latch exception");
			}
		}
		Stat exists = zk.exists(GROUPNODE, false);
		if (exists == null) {
			zk.create(GROUPNODE, "szw-servers is created?".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		}
		exists = zk.exists(SUBNODE, false);
		if (exists == null) {
			// 所有的子节点应该是瞬时节点,但这个节点信息里保存有全部的服务器名称
			zk.create(SUBNODE, "szw-server is created ? ".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
		}
	}

	// 方便启动的时候设置服务器名称
	public ApplicationListener(String serverName) throws Exception {
		connectZkServer(serverName);
		// 连好之后,调用指定任务
		handle();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
		new ApplicationListener(args[0]);
	}

	public void handle() throws Exception {
		System.out.println("----------业务代码处理--------------");
		TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
	}
}
