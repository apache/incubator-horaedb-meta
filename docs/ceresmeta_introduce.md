# Ceresmeta

## 1. Introduction: Why do we need Ceresmeta
When CeresDB is in stand-alone mode, it relies on local storage. The data is stored in local storage. when we create schema and create table, the interaction of data is only focused on the current stand-alone node; while in cluster mode, if each node still uses the stand-alone mode for data storage and data interaction, there are some problems:

- Some nodes have more operations to create schemas and tables, resulting in full physical storage. At this time, data migration needs to be performed. Data migration often occupies a large amount of network bandwidth, which may further affect online service. 
- When data analysis and data processing are concentrated on a small number of nodes, the computing resources and memory resources of these nodes may be overloaded and cannot respond in time, while other nodes are often idle, and the access load is unbalanced, which affects the response of the service time.

Obviously, it is not feasible to use the stand-alone data processing method in cluster mode, so we need a "brain" to manage and schedule the entire CeresDB cluster, and to store and access data. An overall control function that makes the entire CeresDB cluster load balanced.
Ceresmeta is the "brain" we use. Ceresmeta is the central control module of CeresDB, responsible for the management and scheduling of the entire CeresDB cluster.

## 2. CeresDB distributed architecture

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

When CeresDB is in a cluster mode, an important issue we need to consider is how to ensure the load balance of the cluster. As shown in the overall CeresDB distributed architecture schematic diagram above, our Ceresmeta component here is the "brain" responsible for managing our CeresDB cluster.

## 3. Ceresmeta Architecture

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

The above figure is the overall design architecture of Ceresmeta. Ceresmeta communicates with CeresDB through grpc stream, CeresDB reports its own node information to Ceresmeta, and Ceresmeta controls the CeresDB cluster as a whole based on this information. These modules are described in detail below.

### 3.1 server module
The server module is responsible for starting and initializing the Ceresmate service. When we start the Ceresmate service, the server module initializes Ceresmate according to the configuration file, which mainly includes:

- Initialize the underlying storage service:ETCD;
- Call the grpc server module to initialize the grpc service;
- Call the election module to initialize the Ceresmate node election master service.

### 3.2 election module
To ensure the stability of Ceresmeta's own services, we also adopt a distributed architecture for Ceresmeta. In the distributed case, we need to ensure that the consistency of Ceresmeta scheduling. Here we use the Leader/Follower model, only one node in the Ceresmeta cluster is the Leader, which is responsible for scheduling, and the other nodes are Followers. 

This module is responsible for selecting the leader node on all nodes. Here we are based on the data consistency guaranteed by the underlying storage service ETCD, and select the leader through distributed locks.

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


### 3.3 grpc server module
Ceresmate conducts two-way asynchronous communication with CeresDB through grpc stream. We divide the interactive information into three categories:

- CeresDB reports its own node information to Ceresmeta through heartbeat; 
- the request command uploaded by CeresDB;
-  the scheduling command issued by Ceresmeta.

                               ┌─────────────┐   Grpc stream    ┌─────────────┐
                               │   CeresDB   │ ◀──────────────▶ │  Ceresmeta  │
                               └─────────────┘                  └─────────────┘


In addition, because we adopt a distributed architecture for Ceresmate, we need to ensure that the request is processed by the leader node. Therefore, under the grpc server module, we also implement the request forwarding function. If the Follower node receives a request from CeresDB, it will forward the request to the Leader node for processing. The processed result is sent to the original Follower node, and the Follower node sends the processed result to CeresDB.

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


### 3.4 manager module
A Ceresmeta service can manage multiple CeresDB clusters, so we abstracted the manager module, which is responsible for managing multiple CeresDB clusters. The manager module provides the grpc module with an interface for processing requests, and calls down the specific implementation of the cluster module to complete the processing of these requests. 

In addition, since the manager module manages multiple CeresDB clusters, the cluster ID assignment is also implemented by this module. The manager module will call the ID Allocator module to assign a unique cluster ID to each CeresDB cluster.

### 3.5 cluster module
The cluster module is responsible for the management of the overall cluster information and the specific implementation of processing requests. The cluster module depends on the storage module and the schedule module. When initializing the Ceresmeta service, the cluster module will load all cluster information from storage into memory, and with cluster Changes, synchronously change the cluster state information in memory and storage.

The cluster module is also responsible for the implementation of incoming CeresDB request processing. In the process of processing some requests, the cluster will call the schedule module to process it to ensure the load balance of the CeresDB cluster.

### 3.6 schedule module
The schedule module is the most core and complex module in Ceresmeta. The fundamental purpose of our Ceresmeta service is to ensure the load balancing of the CeresDB cluster. The schedule module is the implementation of the specific scheduling algorithm, which will make adjustments and scheduling according to the status information of the cluster to ensure that the cluster is stable load balancing. 

At present, there is only one scheduling algorithm we have implemented: to evenly distribute the CeresDB data on all nodes in the cluster, we currently use the method of random hash distribution.

### 3.7 storage module
The storage module is responsible for the storage of CeresDB cluster meta information, and provides the cluster module with an operation interface for meta information. 

The underlying storage service we use is ETCD, which ensures data consistency in distributed situations.

## 4. Summary
Ceresmeta is an indispensable part of distributed CeresDB, responsible for storing the real-time data distribution of 
each CeresDB node and the overall topology of the cluster. Ceresmeta is not just a simple meta-information storage, at 
the same time, Ceresmeta will grasp the CeresDB cluster status as a whole based on the data distribution status reported 
by CeresDB nodes in real time, make scheduling, and ensure the load balance of the cluster. It can be said to be 
the "brain" of the entire cluster. 