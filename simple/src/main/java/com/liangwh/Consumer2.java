package com.liangwh;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer2 {



    public static void main(String[] args) throws Exception{
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LIANG_CONSUMER_GROUP3");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("k2","*");
        /**
         * MessageListenerOrderly 有序初一
         * MessageListenerConcurrently 同时处理
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {


            @SneakyThrows
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    for(MessageExt msg : list){

                        StringBuilder sb = new StringBuilder();
                        sb.append("----------------222---------").append("\r\n");
                        sb.append(msg).append("\r\n");
                        sb.append(msg.getQueueId()).append("\r\n");
                        sb.append(new java.lang.String(msg.getBody())).append("\r\n");
                        System.out.println(sb.toString());
                        //int i= 1/0;
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }finally {
                    System.out.println("我草");
                }



            }

            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for(MessageExt msg : list){
                    StringBuilder sb = new StringBuilder();
                    sb.append("-------------------------").append("\r\n");
                    sb.append(msg).append("\r\n");
                    sb.append(new java.lang.String(msg.getBody())).append("\r\n");
                    System.out.println(sb.toString());
                }


                return ConsumeOrderlyStatus.SUCCESS;
            }


        });
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.start();
    }
}
