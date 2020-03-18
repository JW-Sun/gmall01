package com.real.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.real.GmallConstants;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) {

        CanalConnector canalConnect = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.159.102", 11111), "example", "", "");

        for (;;) {
            canalConnect.connect();
            canalConnect.subscribe("gmall01.*");

            //抓取100个放入message
            Message message = canalConnect.get(100);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("无数据，停5s");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("处理数据");
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //如果是事务的开启或者关闭的事件，跳过这个.
                    //判断事件类型只处理 行变化
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        ByteString storeValue = entry.getStoreValue();
                        //将entry进行反序列化成rowchange
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        //需要得到行集

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //获得表的名称
                        String tableName = entry.getHeader().getTableName();

                        handler(tableName, eventType, rowDatasList);
                    }
                }
            }
        }

    }


    /***
     *对结果集进行计算
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //下单操作
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_NEW_ORDER);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //订单明细
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            //当用户发生插入或者更新的时候的情况
            sendToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER_INFO);
        }
    }


    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //在每一个行集中获得列的相关数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            //列集打印
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() +" "+ column.getValue());
                /*将每一行每一列的情况发送到kafka*/
                jsonObject.put(column.getName(), column.getValue());
            }
            MyKafkaProducer.send(topic, jsonObject.toJSONString());

            try {
                Thread.sleep(1000 * new Random().nextInt(3));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



}
