nohup sh bin/mqbroker -n localhost:9876 >/Users/apple/tool/rocketmq-all-4.7.1-bin-release/mylog/broker.log  2>&1
 
nohup sh bin/mqnamesrv  >/Users/apple/tool/rocketmq-all-4.7.1-bin-release/mylog/namesrv.log  2>&1 &
#/Users/apple/store
#/Users/apple/logs/rocketmqlogs
#/Users/apple/logs/rocketmqlogs/broker.log 会打印默认配置
#/Users/apple/logs/rocketmqlogs/namesrv.log 会打印默认配置
#&
