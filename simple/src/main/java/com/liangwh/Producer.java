package com.liangwh;



import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;

public class Producer {

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
        msg1.putUserProperty("age","1");
//        Message msg2 = new Message("k2","hellow,i am k1 two".getBytes());
//        msg2.putUserProperty("age","2");
//
//        Message msg3 = new Message("k2","hellow,i am k1 three".getBytes());
//        msg3.putUserProperty("age","3");
//
//        Message msg4 = new Message("k2","hellow,i am k1 fore".getBytes());
//        msg4.putUserProperty("age","4");

        msg1.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,"7F0000011B8018B4AAC20CBFFA140000");
        SendResult send = producer.send(msg1,3000*9L);
        MessageExt k2 = producer.getDefaultMQProducerImpl().queryMessageByUniqKey("k2", "7F0000012E7418B4AAC20CBAE1530000");
        //System.out.println(k2);
        ArrayList<Message> msgList = new ArrayList<Message>();

//        msgList.add(msg2);
//        msgList.add(msg3);
//        msgList.add(msg4);

//        send = producer.send(msgList);
        System.out.println(send);
        producer.shutdown();

    }
}
