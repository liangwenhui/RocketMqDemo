package com.liangwh;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {


    /**
     * 消费模式是消费者客户端自己配置的，与消费组无关，与broker无关（我觉得是每一个消费者自己维护了偏移量）
     * 集群模式， consumer.setMessageModel(MessageModel.CLUSTERING); 默认
     * 广播模式         consumer.setMessageModel(MessageModel.BROADCASTING);topic中的消息都会消费
     * 只消费一次
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LIANG_CONSUMER_GROUP3");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("k2","*");
        /**
         * MessageListenerOrderly 有序初一
         * MessageListenerConcurrently 同时处理
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {


            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
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
