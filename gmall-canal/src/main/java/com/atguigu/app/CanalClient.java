package com.atguigu.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

import static com.atguigu.handle.CanalHandler.handle;

/**
 * @Author: Sdaer
 * @Date: 2020-08-18
 * @Desc:
 *        本类的作用
 *        1、获取Canal连接
 *        2、循环监控获取数据
 *
 *        Canal信息架构
 *        Message：一次canal从日志榨取的信息，一个message可以包含多个sql执行的结果
 *          Entry：对应一个sql命令，一个sql可能会对多行记录造成影响
 *              TableName(表名)
 *              EntryType(实例类型)：ROWDATA(我们要用的)
 *              StoreValue(存储的数据)：此数据为序列号的数据不可直接使用
 *
 *              StoreValue反序列化后得到RowChange
 *              RowChange：
 *                  EventType：DDL，DCL关键字等
 *                  RowDataList：行数据列表(包含多行数据)
 *                      RowData:(一行数据)
 *                          Column：列
 *
 *
 */
public class CanalClient {

    public static void main(String[] args) {

        //1.获取Canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),   //Canal地址及端口
                "example",
                "",
                "");

        //2.循环监控获取数据
        while (true){

            canalConnector.connect();   //建立canal连接
            canalConnector.subscribe("gmall200317.*");    //订阅监控数据库

            Message message = canalConnector.get(100);  //每次拉取100条数据，少于全拉，多于只拉100

            if (message.getEntries().size() <= 0){
                System.out.println("无数据，休息5秒");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {

                //1.获取message中的Entry集合并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //2.获取entry中RowData类型的数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){

                        try {
                            //1.获取表名
                            String tableName = entry.getHeader().getTableName();
                            //2.获取数据
                            ByteString storeValue = entry.getStoreValue();
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //3.获取行数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            //4.获取数据操作类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //5.根据不同的表，处理数据
                            handle(tableName,eventType,rowDatasList);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }


                    }

                }

            }


        }

    }


}
