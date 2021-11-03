package com.liangwh;



import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;

import java.util.ArrayList;

public class ProducerTag {

    /**
     * 同步投递
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("LIANG_PRODUCER_GROUP");

        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setSendMessageWithVIPChannel(false);
        producer.start();


        Message msg1 = new Message("k2","hellow,i am k1 one 2".getBytes());

        SendResult send = producer.send(msg1,3000*9L);
        System.out.println(send);
         send = producer.send(msg1,3000*9L);

        System.out.println(send);
        producer.shutdown();

    }
}
