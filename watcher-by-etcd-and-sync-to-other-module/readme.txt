
考虑下面的情况，一个系统中有多个模块，多个模块独立部署，其中有一个用户模块，统一负责用户信息的存储，比如放到etcd中
    而另一个模块，比如鉴权模块，需要用户信息来判断该用户是否在系统中，如果不在需要提示用户注册，
    如果调用用户模块的接口来进行，效率比较慢，系统可以在内存中也保留一份用户信息，因此需要同步用户信息
    这时候有两种方法，一种是监听etcd，这种方法会导致用户模块监听了etcd的用户信息，鉴权模块也监听了etcd的用户信息，不太合理
    另一种方法就是通过rpc监听用户模块，从用户模块同步用户信息，类似k8s中的api-service实现，该模块统一访问etcd，本系统中由用户模块统一访问etcd

proto文件的生成，在项目目录下，即/Users/wanglu51/goland_project/src/myself/go-tools目录中，执行命令：
    protoc --go_out=. watcher-by-etcd-and-sync-to-other-module/pb/user.proto
    protoc --go_out=. --go-grpc_out=. watcher-by-etcd-and-sync-to-other-module/pb/rpc.proto

运行方式：
    1. 运行watcher-by-etcd-and-sync-to-other-module/main.go，启动一个窗口watcher-by-etcd-and-sync，观察日志输出，会有loaded base data等日志，打印出从etcd中获取到用户信息
    2. 因为有了grpc服务，所以不能在本地再启动一个窗口了，否则端口冲突了，因此只使用一个窗口观察
    3. 运行watcher-by-etcd-and-sync-to-other-module/auth-as-other-module/main.go，启动一个窗口auth-as-other-module1
    4. 运行watcher-by-etcd-and-sync-to-other-module/auth-as-other-module/main.go，启动一个窗口auth-as-other-module2
    5. 运行watcher-by-etcd-and-sync-to-other-module/tools/addUser.go，启动一个窗口，添加一个用户信息到etcd中
    6. 观察窗口watcher-by-etcd-and-sync，看新增的用户信息是否同步到本模块的实例中
    7. 观察窗口auth-as-other-module1和auth-as-other-module2中的输出，看新增的用户信息是否同步到其他模块的实例中

