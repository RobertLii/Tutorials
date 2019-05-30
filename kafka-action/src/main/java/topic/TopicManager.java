package topic;


import kafka.zk.AdminZkClient;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Arrays;
import java.util.Properties;

public class TopicManager {
    private static final String ZK_CONNECT = "192.168.80.129:2181";
    private static final int SESSION_TIMEOUT = 10000;
    private static final int CONNECT_TIMEOUT = 10000;
    public static void createTopic(String topic, int partition, int replica, Properties props) {
        //ZkClient zkClient = new ZkClient(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT);
        AdminZkClient adminZkClient;
        KafkaAdminClient kafkaAdminClient;
        ClientUtils clientUtils;

        AdminClient adminClient = AdminClient.create(props);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        //adminClient.deleteTopics(Arrays.asList("stock-quotations"));
        //adminClient.describeTopics(Arrays.asList("stock-quotation"));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic(topic, partition, (short)replica)));

        try {
            listTopicsResult.listings().get();
            createTopicsResult.all().get();
        } catch (Exception e) {
            System.out.println("create topic failed!");
            e.printStackTrace();
        } finally {
            //zkUtils.close();
        }
    }

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.80.129:9092");
        props.put("create.topic.policy.class.name", CreateTopicPolicy.class.getName());

        return  props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();

        createTopic("stock-quotation", 1, 1, props);
    }
}
