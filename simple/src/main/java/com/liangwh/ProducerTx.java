package com.liangwh;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 事务消息，半消息
 */
public class ProducerTx {


    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("LIANG_TX_P_GROUP");
        producer.setNamespace(System.getProperty("namesrvAddrs"));
        producer.setSendMessageWithVIPChannel(false);
        producer.start();
        Message message  = new Message("tx1","","","adasdasda".getBytes());
        producer.setTransactionListener(new TransactionListener() {
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                System.out.println("do sss");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            /*
            当exe方法返回UNKNOW的时候，broker会间隔一段时间，回调check方法
             */
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                return null;
            }
        });

    }
}
