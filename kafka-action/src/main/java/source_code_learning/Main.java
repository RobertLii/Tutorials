package source_code_learning;

import consumer.KafkaSimpleConsumer;
import kafka.Kafka;
import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.tools.GetOffsetShell;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zk.ZkData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.HashMap;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Kafka.main(args);
        KafkaServerStartable kafkaServerStartable;
        KafkaServer kafkaServer;
        KafkaZkClient kafkaZkClient;
        KafkaConfig kafkaConfig;
        ZkData zkData;


        AdminClient adminClient = AdminClient.create(new Properties());
        AdminClientConfig adminClientConfig = new AdminClientConfig(new HashMap<>());
        CommonClientConfigs commonClientConfigs = null;

        GetOffsetShell getOffsetShell;
        GroupMetadataManager.GroupMetadataMessageFormatter groupMetadataMessageFormatter;
    }

    public static void topic() {
        KafkaSimpleConsumer kafkaSimpleConsumer = null;
        MetadataRequest metadataRequest = null;
        MetadataResponse metadataResponse = null;
        MetadataRequestData metadataRequestData = null;
        MetadataResponseData metadataResponseData = null;
        MetadataRequestData.MetadataRequestTopic metadataRequestTopic;

        TopicAndPartition topicAndPartition;
    }
}
