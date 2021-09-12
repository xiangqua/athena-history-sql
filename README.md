# athena_history_sql

增强Amazon Athena对历史查询记录的统计分析功能
背景介绍
Amazon Athena 是一种交互式查询服务，让您能够轻松使用标准 SQL 分析 Amazon S3 中的数据。Athena 采用无服务器架构，因此您无需管理任何基础设施，且只需为您运行的查询付费。Athena 简单易用，只需指向您存储在 Amazon S3 中的数据，定义架构并使用标准 SQL 开始查询。就可在数秒内获取最多的结果。您可以使用Athena 控制台查看成功和失败的查询、下载成功查询结果文件以及查看失败查询的错误详细信息。Athena 将查询历史记录保留 45 天。如果要将查询历史记录保留超过 45 天，您可以检索查询历史记录并将其保存到等 Amazon S3 等数据存储中。本方案自动执行此过程，使用 Athena 和 Amazon S3 API将数据导入到Amazon S3，并使用Athena分析历史数据，结合Amazon CloudTrail的Athena API调用日志可以对Athena的历史SQL执行记录进行多个维度的分析，以便用于评估Athena的使用和优化线上SQL等。
部署架构
利用CloudWatch Event定时触发Lambda代码同步Athena历史查询日志到Amazon S3，利用DynamoDB记录每次增量同步的记录位置，Amazon CloudTrail记录Athena API call日志，创建CloudTrail的跟踪，将日志持续导入到S3中，最终通过Athena多维分析历史查询日志和CloudTrail日志并利用Amazon QuickSight进行图表展现。
 
# 方案部署步骤
导出Athena查询历史记录日志到Amazon S3
因为Athena历史查询记录日志只保留45天，我们通过一段Python代码将Athena历史查询记录日志持续的，增量的导入到Amazon S3中，利用DynamoDB记录每次导出的最近位置，下次导出的时候，从上次导出的最新位置开始增量导出，避免产生重复数据，我们也可以将这段代码部署在在Lambda上，通过CloudWatch Event定时触发同步日志到Amazon S3。
•	创建DynamoDB表，保存每次增量导入的最新位置
表名称：athena_his_sql
主分区键：workgroup
 
•	创建Lambda函数athena_his_fun
将下面的脚本复制到Lambda的入口脚本lambda_function.py（Lambda函数执行的角色需要具备操作Amazon S3读写，DynamoDB读写的权限）并修改Lambda的内存（500M）和超时时间（10min）。

)

部署完毕后设置利用CloudWatch Event定时触发执行（例如按小时触发）
 
脚本执行后，会在DynamoDB的表athena_his_sql中更新当前最新的查询ID，方便后续增量导出。
 

利用Amazon CLI或者控制台检查Amazon S3路径下是否正确上传了日志文件，（注：本方案没有对上传到S3的数据进行分区存放，可以参考下文CloudTrail日志的方式利用Athena的分区投影功能实现自动分区管理）。
 
# 创建Athena历史记录日志表
CREATE EXTERNAL TABLE athena_queries (
    QueryExecutionId string,
    Query string,
    StatementType string,
    Status struct<State:string,SubmissionDateTime:string,CompletionDateTime:string>,
    Statistics struct<EngineExecutionTimeInMillis:int,DataScannedInBytes:int, TotalExecutionTimeInMillis:int, QueryQueueTimeInMillis:int, QueryPlanningTimeInMillis:int, ServiceProcessingTimeInMillis:int>,
    WorkGroup string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://quandata1/athena_his';
创建CloudTrail日志表
开启CloudTrail跟踪，将CloudTrail日志通过跟踪功能持续保存到S3中。
  
# 创建CloudTrail日志表cloudtrail_logs，建表语句中LOCATION根据实际跟踪配置的S3路径填写。使用Athena分区投影功能自动进行分区管理，降低查询时间和数据扫描量。
CREATE EXTERNAL TABLE cloudtrail_logs(
    eventVersion STRING,
    userIdentity STRUCT<
        type: STRING,
        principalId: STRING,
        arn: STRING,
        accountId: STRING,
        invokedBy: STRING,
        accessKeyId: STRING,
        userName: STRING,
        sessionContext: STRUCT<
            attributes: STRUCT<
                mfaAuthenticated: STRING,
                creationDate: STRING>,
            sessionIssuer: STRUCT<
                type: STRING,
                principalId: STRING,
                arn: STRING,
                accountId: STRING,
                userName: STRING>>>,
    eventTime STRING,
    eventSource STRING,
    eventName STRING,
    awsRegion STRING,
    sourceIpAddress STRING,
    userAgent STRING,
    errorCode STRING,
    errorMessage STRING,
    requestParameters STRING,
    responseElements STRING,
    additionalEventData STRING,
    requestId STRING,
    eventId STRING,
    readOnly STRING,
    resources ARRAY<STRUCT<
        arn: STRING,
        accountId: STRING,
        type: STRING>>,
    eventType STRING,
    apiVersion STRING,
    recipientAccountId STRING,
    serviceEventDetails STRING,
    sharedEventID STRING,
    vpcEndpointId STRING
  )
PARTITIONED BY (
   `timestamp` string)
ROW FORMAT SERDE 'com.amazon.emr.hive.serde.CloudTrailSerde'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket/AWSLogs/account-id/CloudTrail/aws-region'
TBLPROPERTIES (
  'projection.enabled'='true', 
  'projection.timestamp.format'='yyyy/MM/dd', 
  'projection.timestamp.interval'='1', 
  'projection.timestamp.interval.unit'='DAYS', 
  'projection.timestamp.range'='2021/01/01,NOW', 
  'projection.timestamp.type'='date', 
  'storage.location.template'='s3://bucket/AWSLogs/account-id/CloudTrail/aws-region/${timestamp}')

#使用Athena对数据进行分析
查看不同SQL语句的执行总次数排名
select count(*),query from athena_queries group by query order by 1 desc limit 10

查看执行状态失败的SQL总数
select count(*) from athena_queries where status.state='FAILED'

查看执行超过特定执行时长的历史SQL
select query from athena_queries where statistics.totalexecutiontimeinmillis >=5000

查看超过特定数据扫描量的历史SQL
select query from athena_queries where statistics.datascannedinbytes >=10741612544

根据IAM用户统计数据扫描量
select sum(b.statistics.datascannedinbytes),
a.userIdentity.username 
from 
cloudtrail_logs a,
athena_queries b 
where 
a.eventsource='athena.amazonaws.com' and a.eventName='StartQueryExecution' and 
a.responseElements != 'null' and substr(a.responseElements,22,36) = b.queryexecutionid 
group by 
a.userIdentity.username
使用Amazon Quicksight可视化分析结果
利用SQL的方式（将cloudtrail_logs和athena_queries两张表联表查询）创建QuickSight中Athena数据集，然后根据实际需要在Amazon QuickSight创建可视化图表。
select 
b.queryexecutionid,
b.query,
b.statementtype,
b.status.State,
b.status.SubmissionDateTime,
b.status.CompletionDateTime,
b.statistics.EngineExecutionTimeInMillis,
b.statistics.DataScannedInBytes,
b.statistics.TotalExecutionTimeInMillis,
b.statistics.QueryQueueTimeInMillis,
b.statistics.QueryPlanningTimeInMillis,
b.statistics.ServiceProcessingTimeInMillis,
b.workgroup,
a.userIdentity.username
from 
cloudtrail_logs a,
athena_queries b 
where 
a.eventsource='athena.amazonaws.com' and a.eventName='StartQueryExecution' and 
a.responseElements != 'null' and substr(a.responseElements,22,36) = b.queryexecutionid 
创建可视化图表如下
 
总结
通过本文介绍的方案可以更长时间的保留Athena查询历史日志，通过对Athena历史查询日志的分析，让我们可以直观的了解和掌握Athena的使用细节，查看Top SQL，检测应用性能问题，在时间、用户、SQL语句等多个维度增强对Athena使用的洞察。


