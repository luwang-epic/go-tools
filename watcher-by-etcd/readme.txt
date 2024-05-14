
通过监听etcd，确保模块中某个对象的信息在各个实例中一致

proto文件的生成，在项目目录下，即/Users/wanglu51/goland_project/src/myself/go-tools目录中，执行命令：
    protoc --go_out=. watcher-by-etcd/pb/user.proto

运行方式：
    1. 运行watcher-by-etcd/main.go，启动一个窗口watcher-by-etcd1，观察日志输出，会有loaded base data等日志，打印出从etcd中获取到用户信息
    2. 运行watcher-by-etcd/main.go，启动一个窗口watcher-by-etcd2，观察日志输出，会有loaded base data等日志，打印出从etcd中获取到用户信息
    3. 运行watcher-by-etcd/tools/addUser.go，启动一个窗口2，添加一个用户信息到etcd中
    4. 观察窗口watcher-by-etcd1和watcher-by-etcd2中的输出，看新增的用户信息是否同步到对应的实例中

