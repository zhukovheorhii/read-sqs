package org.example;

import com.amazonaws.services.sqs.model.Message;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

 class BaseTest {
    @Test
    void simpleTest() {
        final String topicArn = "arn:aws:sns:us-west-2:...:...";
        String queue = SQSUtils.createSqs();

        //if isSubscriptionExtended == true we can subscribe dew SNS to the same topic
        SQSUtils.subscribeQueue(topicArn, queue, true);

        List<JSONObject> events = filterSqsEvents(queue, 10);
        assertTrue(events.size() > 0, "Queue is empty!");
    }


    public static List<JSONObject> filterSqsEvents(String queueUrl, int timeout) {
        List<Message> eventsList = SQSUtils.getSqsEvents(timeout, queueUrl);
        //Clear messages if target message was not found. This is required as we are fetching 10 messages per request
        //And it gives ability to avoid fetch same 10 messages
        SQSUtils.batchMessageDeleteByAwsUtils(queueUrl, eventsList);
        return SQSUtils.getEventsBody(eventsList)
                .stream()
                .filter(ec -> ec.get("someField").toString().equals("someString"))
                .collect(Collectors.toList());
    }
}
