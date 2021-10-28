package org.example.sqs;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Main {
    public static void main(String[] args) {

        String name = "test-delay-q";

        // queue name
        String queueName = name + UUID.randomUUID().toString().replace("-", "");

        try {

            // create client
            SqsClient sqsClient = SqsClient.builder()
                    .region(Region.EU_CENTRAL_1)
                    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .build();

            // create the queue
            CreateQueueRequest.Builder request = CreateQueueRequest.builder().queueName(queueName);
            Map<QueueAttributeName, String> attributes = new HashMap();
            attributes.put(QueueAttributeName.DELAY_SECONDS, String.valueOf(30));
            request.attributes(attributes);

            CreateQueueResponse queueResult = sqsClient.createQueue(request.build());
            String queueUrl = queueResult.queueUrl();

            System.out.println(queueUrl);


            // list the queue
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(name).build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            for (String url : listQueuesResponse.queueUrls()) {
                System.out.println(url);
            }


            // sending respecting the delay of the queue
            //SendMessageRequest.Builder sendMessageRequest = SendMessageRequest.builder().queueUrl(queueUrl).messageBody("hello");

            // sending ignoring the delay of the queue, setting the delay to 0s
            SendMessageRequest.Builder sendMessageRequest =  SendMessageRequest.builder().queueUrl(queueUrl).delaySeconds(0).messageBody("hello");
            SendMessageResponse result = sqsClient.sendMessage(sendMessageRequest.build());

            ReceiveMessageRequest.Builder receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl);
            ReceiveMessageResponse messageResult = sqsClient.receiveMessage(receiveRequest.build());
            for (Message message : messageResult.messages()) {
                System.out.println(message.body());
            }


        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } finally {

        }
    }
}
