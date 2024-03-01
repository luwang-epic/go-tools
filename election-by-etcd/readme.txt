
通过etcd进行选取的工具

运行方式：
    启动一个窗口，比如election-by-etcd-1, 看是否成为leader
    在启动一个窗口，比如election-by-etcd-2，看是否不是leader
    再将election-by-etcd-1的进程关闭，看election-by-etcd-2是否成为leader
    再重新启动election-by-etcd-1，看是否不是leader