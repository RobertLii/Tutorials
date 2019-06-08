package topic;


import kafka.admin.TopicCommand;
import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicManager {

    public static void createTopic(String topic, int partition, int replica, Properties props) {
        AdminClient adminClient = AdminClient.create(props);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList("stock-quotation"));
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("stock-quotation"));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(new NewTopic(topic, partition, (short)replica)));

        try {
            listTopicsResult.listings().get();
            deleteTopicsResult.all().get();
            createTopicsResult.all().get();
        } catch (Exception e) {
            System.out.println("create topic failed!");
            e.printStackTrace();
        } finally {
            //zkUtils.close();
        }
    }

    public static void listTopics(Properties properties) {
        AdminClient adminClient = AdminClient.create(properties);

        ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResultWithOptions = adminClient.listTopics(new ListTopicsOptions().timeoutMs(6000));
        try {
            Collection<TopicListing> collections = listTopicsResult.listings().get();
            for (TopicListing topicListing : collections) {
                System.out.printf(topicListing.toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put("create.topic.policy.class.name", CreateTopicPolicy.class.getName());

        return  props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();

        createTopic("stock-quotation", 1, 1, props);
        listTopics(props);

    }
}
