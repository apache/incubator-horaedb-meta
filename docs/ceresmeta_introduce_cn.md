# Ceresmeta

## 1.引言:为什么需要 Ceresmeta

当 CeresDB 为单机模式时，是依赖于本地存储来完成的，将数据存储在本地的存储中，实现 create schema 和 create table ，数据的交互也仅仅集中于当前的单机 node；而在集群模式下，若每个 node 仍然使用单机模式的处理方式来进行数据的存储，数据交互，则存在一些问题：

- 某些 node 的 create schema 和 create table 操作较多，导致物理存储存满，而这时候就需要进行数据的迁移，数据的迁移往往会占用大量的网络带宽，进一步可能影响到线上的服务。
- 数据分析，数据处理集中于少量 node 时，则这些 node 的计算资源，内存资源可能处于超负荷状态，不能及时的进行响应，而其他 node 往往处于空闲状态，访问负载不均衡，影响服务的响应时间。

显然在集群模式下使用单机模式的数据处理方式，是不可行的，因此我们需要一个“大脑”，对整个 CeresDB 集群进行管理、调度，对数据的存储，数据的访问起到一个整体控制作用，使得整个 CeresDB 集群负载均衡。
Ceresmeta 就是我们使用的“大脑”，Ceresmeta 是 CeresDB 的中心总控模块，负责整个 CeresDB 集群的管理、调度。

## 2.CeresDB 分布式架构
                                    ┌─────────────┐
                                    │   client    │                   
                                    └─────────────┘
                                           │                                         
                                           │                                         
                                           ▼          
                              ┌──────────────────────────┐                
                              │   ┌──────┐    ┌──────┐   │        
                              │   │ node │    │ node │   │          ┌─────────────┐
                      CeresDB │   └──────┘    └──────┘   │ ───────▶ │  Ceresmeta  │
                      cluster │   ┌──────┐    ┌──────┐   │          └─────────────┘
                              │   │ node │    │ node │   │             
                              │   └──────┘    └──────┘   │
                              └──────────────────────────┘   
                                           │                                         
                                           │                                         
                                           ▼     
                                    ┌─────────────┐
                                    │ storage:OSS │                   
                                    └─────────────┘

当 CeresDB 是以集群的方式进行部署时，我们所需要考虑的一个重要问题就是就是如何保证集群的负载均衡。
如上图所展示的整体的 CeresDB 分布式架构示意图，我们这里的 Ceresmeta 组件，就是作为“大脑”，负责管理我们的 CeresDB 集群。

## 3.Ceresmeta 架构
                                     ┌────────────────────────────────┐
                                     │ ┌────────────────────────────┐ │
                                     │ │           Server           │ │                  
                                     │ └────────────────────────────┘ │
                                     │ ┌─────────────┐┌─────────────┐ │
                                     │ │   Election  ││ Grpc server │ │
                                     │ └─────────────┘└─────────────┘ │
                                     │ ┌────────────────────────────┐ │
                                     │ │          Manager           │ │
                                     │ └────────────────────────────┘ │
                                     │ ┌────────┐┌────────┐┌────────┐ │
                                     │ │ Cluster││ Cluster││ Cluster│ │
                                     │ └────────┘└────────┘└────────┘ │
                                     │ ┌─────────────┐┌─────────────┐ │
                                     │ │   Storage   ││   Schedule  │ │
                                     │ └─────────────┘└─────────────┘ │
                                     └────────────────────────────────┘ 
                                                  Ceresmeta

上图是 Ceresmeta  的整体设计架构图，Ceresmeta 通过 grpc stream 与 CeresDB 进行数据通信，CeresDB 将自身 node 信息上报给 Ceresmeta，Ceresmeta 根据这些信息对 CeresDB 集群进行整体上的控制。下面对这些模块做一下详细说明。

### 3.1 server  模块
server 模块负责 Ceresmate 服务的启动，初始化。当我们启动 Ceresmate 服务时，server 层根据配置文件进行 Ceresmate 初始化，主要包括：

- 初始化底层存储服务 ETCD；
- 调用 grpc server 模块初始化 grpc 服务；
- 调用 election 模块初始化 Ceresmate node 选主服务。

### 3.2 election 模块
为保证 Ceresmeta 自身服务的稳定性，我们对 Ceresmeta 也采用分布式架构。分布式的情况下我们需要保证 Ceresmeta 调度的一致性，这里我们采用的是 Leader/Follower 模型，Ceresmeta 集群中只有一个 node 是Leader，负责进行调度，其他 node 为 Follower。
而本模块就是负责在所有的 node 上选出 Leader node（选主），我们这里是基于底层存储服务 ETCD 保证的数据一致性，通过分布式锁进行选主。

                                          ┌──────────────────────────┐                
                                          │         ┌──────┐         │        
                                          │         │  L   │         │     
                                          │         └──────┘         │ 
                                          │   ┌──────┐    ┌──────┐   │          
                                          │   │  F   │    │  F   │   │             
                                          │   └──────┘    └──────┘   │
                                          └──────────────────────────┘ 
                                                   Ceresmeta
                                                    cluster

### 3.3 grpc server 模块
Ceresmate 通过 grpc stream 与 CeresDB 进行双向异步通信，我们把交互的信息分成三类：

- CeresDB 通过 heartbeat 上报自身节点信息给 Ceresmeta；
- CeresDB 上传的请求命令；
- Ceresmeta 下发的调度命令。

                               ┌─────────────┐   Grpc stream    ┌─────────────┐
                               │   CeresDB   │ ◀──────────────▶ │  Ceresmeta  │
                               └─────────────┘                  └─────────────┘

除此之外，由于我们对 Ceresmate 采用分布式的架构，对于来着 CeresDB 的请求命令，我们需要保证命令是 Leader node 进行处理的，因此在 grpc server 模块下我们还实现了命令的请求转发功能，若是 Follower node 接收到来自 CeresDB 的命令请求，则会将请求转发给 Leader node 进行处理。处理得到的结果发送给原来的 Follower node，由 Follower node 将处理结果发送给 CeresDB。

                                          ┌──────────────────────────────────┐                
                                          │             ┌──────┐             │        
                                          │             │  F   │             │     
                                          │             └──────┘             │ 
                                   request│   ┌──────┐  request   ┌──────┐   │          
                                 ◀────────│──▶│  F   │ ◀────────▶ │  L   │   │             
                                   reponse│   └──────┘  response  └──────┘   │
                                          └──────────────────────────────────┘ 
                                                       Ceresmeta
                                                        cluster

### 3.4 manager 模块
一个 Ceresmeta 服务可以管理多个 CeresDB 集群，因此我们抽象出了 manager 模块，负责对多个 CeresDB 集群进行管理，manager 模块向上向 grpc 模块提供处理请求的接口，向下调用 cluster 模块的具体实现来完成对这些请求的处理。
另外，由于 manager 层对多个 CeresDB 集群进行管理，集群 ID（cluster ID）分配也是由该层实现的，manager 层会去调用 ID Allocator 模块为每一个 CeresDB 集群分配唯一的 cluster ID。

### 3.5 cluster 模块
cluster 模块负责整体集群信息的管理以及处理请求的具体实现，cluster 模块依赖于 storage 模块和 schedule 模块，在初始化 Ceresmeta 服务时，cluster 模块会从 storage 中加载 CeresDB 集群全部信息到 memory 中，并且随着 CeresDB 集群状态的改变，同步改变 memory 和 storage 中的集群状态信息。
cluster 模块还负责了对来着的 CeresDB 请求处理的具体实现。在处理某些请求的过程中，cluster 会依据 CeresDB 集群的整体状态，调用 schedule 模块进行处理，保证 CeresDB 集群的负载均衡。

### 3.6 schedule 模块
schedule 模块是 Ceresmeta 最为核心和最为复杂的模块，我们 Ceresmeta 服务的根本目的就是保证 CeresDB 集群的负载均衡，schedule 模块是具体调度算法的实现，会依据集群的状态信息作出调整调度，保证集群的负载均衡。
目前我们实现的调度算法只有一个：将集群数据均匀的分布在集群所有的 node 上，我们当前是采用随机散列分布的方法。

### 3.7 storage 模块
storage 模块负责 CeresDB 集群元信息的存储，向 cluster 层提供元信息的存取操作接口。
我们使用的底层存储服务是 ETCD，保证了分布式情况下数据的一致性。

## 4.总结
Ceresmeta 是分布式 CeresDB 不可缺少的一部分，负责存储每个 CeresDB 节点实时的数据分布情况和集群的整体拓扑结构。Ceresmeta 不仅仅是单纯的元信息存储，同时 Ceresmeta 会根据 CeresDB 节点实时上报的数据分布状态，从整体上把握 CeresDB 集群状态，作出调度，保证集群的负载均衡，可以说是整个集群的“大脑”。
