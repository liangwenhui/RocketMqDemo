package com.liangwh;



import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.List;

public class Producer2 {

    /**
     * 异步投递
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("LIANG_PRODUCER_GROUP");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setSendMessageWithVIPChannel(true);
        producer.start();
        Message message = new Message("k2",("async messgee").getBytes());
        for (int i = 0;i<1;i++){
            producer.send(message,new MessageQueueSelector(){
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(1);
                }
            },null,new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.println(Thread.currentThread().getName()+"-"+sendResult);
                }
                public void onException(Throwable e) {
                    System.out.println(e.getMessage());
                }
            });
        }
        Thread.sleep(5000);
        producer.shutdown();
    }
}
