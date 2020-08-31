package org.example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;


public class SQSUtils {
    //IMPORTANT: before starting to work with SNS, make sure aws private key is stored in keychain
    public static final AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_WEST_2.getName()).build();
    private static final AmazonSNS sns = AmazonSNSClientBuilder.standard().withRegion(Regions.US_WEST_2.getName()).build();

    public static String createSqs() {
        final CreateQueueRequest createQueueRequest = new CreateQueueRequest(RandomStringUtils.random(5, true, true));
        return sqs.createQueue(createQueueRequest).getQueueUrl();
    }

    public static void subscribeQueue(String snsTopicArn, String queueUrl, Boolean isSubscriptionExtended) {
        Topics.subscribeQueue(sns, sqs, snsTopicArn, queueUrl, isSubscriptionExtended);
    }

    public static List<Message> getSqsEvents(int timeout, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        receiveMessageRequest
                .withMaxNumberOfMessages(10)//quee allows to read max 10 messages per request
                .withWaitTimeSeconds(timeout);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    public static List<JSONObject> getEventsBody(List<Message> sqsEvent) {
        return sqsEvent.stream().map(body -> new ObjectMapper().convertValue(body.getBody(), JSONObject.class)).collect(Collectors.toList());
    }

    public static void purgeQueue(String queueUrl) {
        PurgeQueueRequest purgeRequest = new PurgeQueueRequest(queueUrl);
        try {
            sqs.purgeQueue(purgeRequest);
        } catch (PurgeQueueInProgressException ignored) {
        }
        try {
            //in according with purge documentation it may take up to 60 seconds for the message deletion process to complete
            Thread.sleep(Duration.ofSeconds(60).toMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void batchMessageDeleteByAwsUtils(String queueUrl, List<Message> eventsList) {
        List<DeleteMessageBatchRequestEntry> entries = eventsList.stream().map(e ->
                new DeleteMessageBatchRequestEntry(RandomStringUtils.random(10, false, true), e.getReceiptHandle())).collect(Collectors.toList());

        sqs.deleteMessageBatch(queueUrl, entries);
    }
}
