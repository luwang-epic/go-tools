
通过kafka进行调度，分为两个模块，一个模块负责调度，一个模块负责处理任务
    master下面的是调度模块，调度模块只能有一个实例运行，一般是在主节点运行，可以通过election-by-etcd选主
    worker下面的是工作模块，处理具体的任务，直接消费kafka的数据，是无状态的

proto文件的生成，在项目目录下，即/Users/wanglu51/goland_project/src/myself/go-tools目录中，执行命令：
    protoc --go_out=. scheduler-by-etcd-and-kafka/pb/alarm.proto

运行方式：
    1. 运行scheduler-by-etcd-and-kafka/master/main.go，启动一个窗口master，观察日志输出，会有start to schedule等日志，表示开始调度任务
    2. 运行scheduler-by-etcd-and-kafka/worker/main.go，启动一个窗口worker，观察日志输出，会有kafka consumer up and running等日志，表示启动了消费者，开始消费任务
    3. 运行scheduler-by-etcd-and-kafka/master/tools/createAlarmPolicy.go，启动一个窗口，添加一个任务到etcd中
    4. 观察窗口master和worker中的输出，看master是否持续调度，看worker是否消费到任务

