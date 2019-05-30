package topic;



import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import kafka.zk.AdminZkClient;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.zookeeper.ZKUtil;

import java.util.Properties;

public class TopicManager {
    private static final String ZK_CONNECT = "localhost:2181";
    private static final int SESSION_TIMEOUT = 10000;
    private static final int CONNECT_TIMEOUT = 10000;
    public static void createTopic(String topic, int partition, int replica, Properties props) {
        ZkClient zkClient = new ZkClient(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
        ZKUtil zkUtil = null;
        AdminZkClient adminZkClient;
        AdminClient adminClient;
        try {
//            AdminUtils.createTopic();
//            if (!AdminUtils.topicExists(zkUtils, topic)) {
//                AdminUtils.createTopic(zkUtils, topic, partition, replica, props, AdminUtils.createTopic$default$6());
//            }
        } catch (Exception e) {
            System.out.println("create topic failed!");
            e.printStackTrace();
        } finally {
            //zkUtils.close();
        }
    }
}
