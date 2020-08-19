# 实时项目学习

## 一、硬件准备

1. CPU : I5 or Better
2. 内存：最少16G(大概率会掉服务，最好24或32)



## 二、框架技术支持

以下为需要的框架及技术

1. Hadoop
2. Zookeeper
3. Kafka
4. Redis
5. Nginx
6. HBase
7. Phoenix
8. SpringBoot
9. Canal
10. ElsaticSearch
11. Kiberna



## 三、项目模块

### 1、gmall-common

> 本模块为放置常量的模块，Kafka的消费者主题以及ES的索引信息名均放置此处

### 2、gmall-logger

>本模块为配置Kafka消费代码的模块
>
>本模块会给日志数据添加时间戳
>
>本模块会将日志数据写入本地
>
>本模块会将日志数据传入Kafka
>
>本模块最终会打成Jar包放置集群运行

### 3、gmall-mocker

>本模块为模拟日志数据生成的模块
>
>可以在本模块配置日志生产的数量等信息

### 4、gmall-realtime

>本模块为实时需求代码的编写模块
>
>所有实时的需求均会在本模块完成
>
>根据实时需求会将数据写入Redis和Phoenix(HBase)

### 5、gmall-publisher

>本模块会编写需求完成后将数据展示给报表部门的数据接口
>
>本模块会将数据写入Spring Boot项目页面，供报表部门获取数据

### 6、gmall-canal

> 本模块为canal组件模块
>
> 本模块会将监控到的数据库变化信息进行处理并传入Kafka

### 7、dw-chart

> 本模块为报表模块
>
> 本模块会将处理好的数据获取到并封装到Echart进行报表展示



## 四、项目需求

### 1、日活需求

### 2、交易额(GMV)需求

### 3、预警需求

### 4、灵活分析需求



## 五、项目启动顺序

### 0、日志数据产存启动

> 0、第二章框架技术按顺序(1-7)启动
>
> 1、gmall-realtime模块中的DauAPP(负责创建连接池，获取连接等)
>
> 2、gmall-mocker模块中的JsonMocker(负责产生数据)

### 1、实时日活显示启动

>0、第二章框架技术按顺序(1-7)启动
>
>1、gmall-logger模块打包到集群，在所有节点启动(使用脚本)
>
>2、gmall-realtime模块中的DauAPP(负责创建连接池，获取连接等)
>
>3、gmall-publisher模块负责发布数据
>
>4、dw-chark模块主程序负责实时显示数据(从phoenix取数据发布到端口)
>
>5、gmall-mocker模块的JsonMocker负责产生实时数据(从端口取数据进行实时显示)

### 2、实时日交易额显示启动

>0、第二章框架技术按顺序(1，2，3，8，6，7)启动
>
>1、gmall-canal模块中的CanalClient(负责监控数据库变化，进行数据同步传输到Kafka)
>
>2、gmall-realtime模块中的OrderApp(负责将Kafka的数据进行转换并存储到Phoenix中)
>
>3、gmall-publisher模块负责发布数据(从phoenix取数据发布到端口)
>
>4、dw-chark模块主程序负责实时显示数据(从端口取数据进行实时显示)

3、优惠券预警显示启动

> 0、第二章框架技术按顺序(2，3，5，8，10，11)启动
>
> 1、gmall-realtime模块下的AlertCouponApp
>
> 2、gmall-mocker模块中的JsonMocker(负责产生数据)
>
> 3、Kibana页面实时显示变化数据



## 六、本项目使用的端口

>hadoop
>
>8020	HDFS传输端口
>
>9870	HDFS页面端口
>
>8088	Yarn页面端口

>Zookeeper
>
>2181	框架访问ZK的通信端口
>
>2888	zk内部通信端口，Leader监听此端口
>
>3888	ZK选举端口

> Kafka
>
> 9092	Kafka集群默认的通信端口

> Redis
>
> 6379	Redis服务端口

> Nginx
>
> 80		Nginx默认端口，如造成冲突，可改

> Hbase
>
> 16010	Hbase集群监控
>
> 16000	RegionServer接入

> Canal
>
> 11111	canal默认端口

> 数据发布(gmall-publisher)接口
>
> 8070

> 数据实时监控访问接口
>
> 8087

> Kibana
>
> 5601	页面访问端口

>ElsaticSearch
>
>9200	Http外部通信
>
>9300	Tcp集群内部通信