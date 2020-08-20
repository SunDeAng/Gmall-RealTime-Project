package com.atguigu.handle;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.constant.GmallConstants;
import com.atguigu.utils.MyKafkaSender;

import java.util.List;

/**
 * @Author: Sdaer
 * @Date: 2020-08-18
 * @Desc: 处理canal获取到的数据
 */
public class CanalHandler {

    //处理数据，根据表明以及时间类型将数据发送值Kafka指定主题
    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //GMV需求，只需要order_info表中的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){

            sendToKafka(rowDatasList,GmallConstants.GMALL_TOPIC_ORDER_INFO);

        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //只要order_detail中的新增数据
            sendToKafka(rowDatasList,GmallConstants.GMALL_TOPIC_ORDER_DETAIL);

        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType) )){

            sendToKafka(rowDatasList,GmallConstants.GMALL_TOPIC_USER_INFO);

        }

    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList,String topic){
        //遍历行级
        for (CanalEntry.RowData rowData : rowDatasList) {

            //创建一个JSON对象用于存放一行数据
            JSONObject jsonObject = new JSONObject();

            //变量修改之后的列集
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(),column.getValue());
            }

            //打印单行数据并写入Kafka
            System.out.println(jsonObject.toString());

            MyKafkaSender.send(topic,jsonObject.toJSONString());

        }
    }


}
