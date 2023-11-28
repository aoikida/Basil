// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * tcptransport.cc:
 *   message-passing network interface that uses TCP message delivery
 *   and libasync
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/tcptransport.h"

#include <google/protobuf/message.h>
#include <event2/thread.h>
#include <event2/bufferevent_struct.h>

#include <cstdio>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <utility>
//#include "lib/threadpool.cc"

const size_t MAX_TCP_SIZE = 100; // XXX
const uint32_t MAGIC = 0x06121983;
const int SOCKET_BUF_SIZE = 1048576;
//const int MAX_EVBUFFER_SIZE = 8192;

using std::pair;

TCPTransportAddress::TCPTransportAddress(const sockaddr_in &addr)
    : addr(addr)
{
    memset((void *)addr.sin_zero, 0, sizeof(addr.sin_zero));
}

TCPTransportAddress *
TCPTransportAddress::clone() const
{
    TCPTransportAddress *c = new TCPTransportAddress(*this);
    return c;
}

bool operator==(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return !(a == b);
}

bool operator<(const TCPTransportAddress &a, const TCPTransportAddress &b)
{
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::ReplicaAddress &addr)
{
        int res;
        struct addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = 0;
        hints.ai_flags    = 0;
        struct addrinfo *ai;
        if ((res = getaddrinfo(addr.host.c_str(), addr.port.c_str(),
                               &hints, &ai))) {
            Panic("Failed to resolve %s:%s: %s",
                  addr.host.c_str(), addr.port.c_str(), gai_strerror(res));
        }
        if (ai->ai_addr->sa_family != AF_INET) {
            Panic("getaddrinfo returned a non IPv4 address");
        }
        TCPTransportAddress out =
            TCPTransportAddress(*((sockaddr_in *)ai->ai_addr));
        freeaddrinfo(ai);
        return out;
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::Configuration &config,
                            int idx)
{
  return LookupAddress(config, 0, idx);
}

TCPTransportAddress
TCPTransport::LookupAddress(const transport::Configuration &config,
                            int groupIdx,
                            int replicaIdx)
{
    const transport::ReplicaAddress &addr = config.replica(groupIdx,
                                                           replicaIdx);
    return LookupAddress(addr);
}

static void
BindToPort(int fd, const string &host, const string &port)
{
    struct sockaddr_in sin;

    // look up its hostname and port number (which
    // might be a service name)
    //
    struct addrinfo hints;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_flags    = AI_PASSIVE;
    struct addrinfo *ai;
    int res;
    if ((res = getaddrinfo(host.c_str(),
			   port.c_str(),
                           &hints, &ai))) {
        Panic("Failed to resolve host/port %s:%s: %s",
              host.c_str(), port.c_str(), gai_strerror(res));
    }
    UW_ASSERT(ai->ai_family == AF_INET);
    UW_ASSERT(ai->ai_socktype == SOCK_STREAM);
    if (ai->ai_addr->sa_family != AF_INET) {
        Panic("getaddrinfo returned a non IPv4 address");
    }
    sin = *(sockaddr_in *)ai->ai_addr;

    freeaddrinfo(ai);

    Debug("Binding to %s %d TCP \n", inet_ntoa(sin.sin_addr), htons(sin.sin_port));

    if (bind(fd, (sockaddr *)&sin, sizeof(sin)) < 0) {
        PPanic("Failed to bind to socket: %s:%d", inet_ntoa(sin.sin_addr),
            htons(sin.sin_port));
    }
}

TCPTransport::TCPTransport(double dropRate, double reorderRate,
			   int dscp, bool handleSignals, int process_id, int total_processes, bool hyperthreading, bool server)
{   
    tp.start(process_id, total_processes, hyperthreading, server);

    lastTimerId = 0;

    // Set up libevent
    evthread_use_pthreads();
    event_set_log_callback(LogCallback);
    event_set_fatal_callback(FatalCallback);

    //イベントループ(event_base)の生成
    libeventBase = event_base_new();


    //tp2.emplace(); this works?
    //tp = new ThreadPool(); //change tp to *
    evthread_make_base_notifiable(libeventBase);

    // Set up signal handler
    if (handleSignals) {
        signalEvents.push_back(evsignal_new(libeventBase, SIGTERM,
                                            SignalCallback, this));
        signalEvents.push_back(evsignal_new(libeventBase, SIGINT,
                                            SignalCallback, this));
        signalEvents.push_back(evsignal_new(libeventBase, SIGPIPE,
                                            [](int fd, short what, void* arg){}, this));

        for (event *x : signalEvents) {
            event_add(x, NULL);
        }
    }
    
    _Latency_Init(&sockWriteLat, "sock_write");
}

TCPTransport::~TCPTransport()
{
  // Old version
    // // XXX Shut down libevent?
    // event_base_free(libeventBase);
    // // for (auto kv : timers) {
    // //     delete kv.second;
    // // }
    // Latency_Dump(&sockWriteLat);
    mtx.lock();
    for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
      bufferevent_free(itr->second);
      tcpAddresses.erase(itr->second);
      //TCPTransportTCPListener* info = nullptr;
      //bufferevent_getcb(itr->second, nullptr, nullptr, nullptr,
      //    (void **) &info);
      //if (info != nullptr) {
      //  delete info;
      //}
      itr = tcpOutgoing.erase(itr);
    }
  // for (auto kv : timers) {
  //     delete kv.second;
  // }
    //Latency_Dump(&sockWriteLat);
    for (const auto info : tcpListeners) {
      delete info;
    }
    mtx.unlock();
    // XXX Shut down libevent?
    event_base_free(libeventBase);
}

void TCPTransport::ConnectTCP(
    //クライアント側
    const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc) {
  Debug("Opening new TCP connection to %s:%d \n", inet_ntoa(dstSrc.first.addr.sin_addr),
        htons(dstSrc.first.addr.sin_port));

    // Create socket
    int fd;         //AF_INET:IPv4によるソケット、SOCK_STREAM:TCP
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket for outgoing TCP connection");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK on outgoing TCP socket");
    }

    // Set TCP_NODELAY
    int n = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failedt to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }


    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = 0;
    info->receiver = dstSrc.second;
    info->replicaIdx = -1; //もともと-1だった
    info->acceptEvent = NULL;

    tcpListeners.push_back(info);

    struct bufferevent *bev =
        bufferevent_socket_new(libeventBase, fd,
                               BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

    //mtx.lock();
    tcpOutgoing[dstSrc] = bev;
    tcpAddresses.insert(pair<struct bufferevent*, pair<TCPTransportAddress, TransportReceiver*>>(bev, dstSrc));
    //mtx.unlock();

    bufferevent_setcb(bev, TCPReadableCallback, NULL,
                      TCPOutgoingEventCallback, info);

    
    Debug("TCPOutgoingEventCallback\n");

    //bufferevent_socket_connectは、ソケットAPIのconnect()もすることができる。
    if (bufferevent_socket_connect(bev,
                                   (struct sockaddr *)&(dstSrc.first.addr),
                                   sizeof(dstSrc.first.addr)) < 0) {
        
        //buffereventを解放する
        bufferevent_free(bev);

        //mtx.lock();
        tcpOutgoing.erase(dstSrc);
        tcpAddresses.erase(bev);
        //mtx.unlock();

        Warning("Failed to connect to server via TCP");
        return;
    }

    // bufferevent_enable : buffereventによる書き込み、読み込みを有効化する
    if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
        Panic("Failed to enable bufferevent");
    }

    // Tell the receiver its address
    struct sockaddr_in sin;
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    TCPTransportAddress *addr = new TCPTransportAddress(sin);

    if (dstSrc.second->GetAddress() == nullptr) {
      dstSrc.second->SetAddress(addr);
    }


    // Debug("Opened TCP connection to %s:%d",
	  // inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port));
    Debug("Opened TCP connection to %s:%d from %s:%d \n",
	  inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port),
	  inet_ntoa(sin.sin_addr), htons(sin.sin_port));
}

void TCPTransport::ConnectTCP_batch(
    //クライアント側
    const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc) {
    Debug("Opening new TCP connection to %s:%d \n", inet_ntoa(dstSrc.first.addr.sin_addr),
        htons(dstSrc.first.addr.sin_port));

    // Create socket
    int fd;         //AF_INET:IPv4によるソケット、SOCK_STREAM:TCP
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket for outgoing TCP connection");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK on outgoing TCP socket");
    }

    // Set TCP_NODELAY
    int n = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failedt to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }


    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = 0;
    info->receiver = dstSrc.second;
    info->replicaIdx = -1; 
    info->acceptEvent = NULL;

    tcpListeners.push_back(info);

    struct bufferevent *bev =
        bufferevent_socket_new(libeventBase, fd,
                               BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

    //mtx.lock();
    tcpOutgoing[dstSrc] = bev;
    tcpAddresses.insert(pair<struct bufferevent*, pair<TCPTransportAddress, TransportReceiver*>>(bev, dstSrc));
    //mtx.unlock();

    //TCPReadableCallback_batchの引数にbevとinfoが入る。
    bufferevent_setcb(bev, TCPReadableCallback_batch, NULL,
                      TCPOutgoingEventCallback, info);

    
    Debug("TCPOutgoingEventCallback\n");

    //bufferevent_socket_connectは、ソケットAPIのconnect()もすることができる。
    if (bufferevent_socket_connect(bev,
                                   (struct sockaddr *)&(dstSrc.first.addr),
                                   sizeof(dstSrc.first.addr)) < 0) {
        
        //buffereventを解放する
        bufferevent_free(bev);

        //mtx.lock();
        tcpOutgoing.erase(dstSrc);
        tcpAddresses.erase(bev);
        //mtx.unlock();

        Warning("Failed to connect to server via TCP");
        return;
    }

    // bufferevent_enable : buffereventによる書き込み、読み込みを有効化する
    if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
        Panic("Failed to enable bufferevent");
    }

    // Tell the receiver its address
    struct sockaddr_in sin;
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    TCPTransportAddress *addr = new TCPTransportAddress(sin);

    if (dstSrc.second->GetAddress() == nullptr) {
      dstSrc.second->SetAddress(addr);
    }


    // Debug("Opened TCP connection to %s:%d",
	  // inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port));
    Debug("Opened TCP connection to %s:%d from %s:%d \n",
	  inet_ntoa(dstSrc.first.addr.sin_addr), htons(dstSrc.first.addr.sin_port),
	  inet_ntoa(sin.sin_addr), htons(sin.sin_port));
}

void
TCPTransport::Register(TransportReceiver *receiver,
                       const transport::Configuration &config,
                       int groupIdx, int replicaIdx)
{   //サーバ側
    Debug("replicaIdx: %d", replicaIdx);
    Debug("config.n:%d", config.n);

    UW_ASSERT(replicaIdx < config.n);
    struct sockaddr_in sin; 

    //const transport::Configuration *canonicalConfig =
    RegisterConfiguration(receiver, config, groupIdx, replicaIdx);

    // Clients don't need to accept TCP connections
    if (replicaIdx == -1) {
	return;
    }

    // Create socket
    Debug("Create Socket\n");

    int fd;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket to accept TCP connections");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK");
    }

    // Set SO_REUSEADDR
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_REUSEADDR on TCP listening socket");
    }

    // Set TCP_NODELAY
    n = 1;
    if (setsockopt(fd, IPPROTO_TCP,
                   TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }


    // Registering a replica. Bind socket to the designated
    // host/port
    const string &host = config.replica(groupIdx, replicaIdx).host;
    const string &port = config.replica(groupIdx, replicaIdx).port;

    Debug("host : %s \n", host.c_str());
    Debug("port : %s \n", port.c_str());

    BindToPort(fd, host, port);

    // Listen for connections
    if (listen(fd, 5) < 0) {
        PPanic("Failed to listen for TCP connections\n");
    }

    // Create event to accept connections
    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = fd;
    info->receiver = receiver;
    info->replicaIdx = replicaIdx;
    info->acceptEvent = event_new(libeventBase,
                                  fd,
                                  EV_READ | EV_PERSIST,
                                  TCPAcceptCallback,
                                  (void *)info);
    event_add(info->acceptEvent, NULL);
    tcpListeners.push_back(info);

    // Tell the receiver its address
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    TCPTransportAddress *addr = new TCPTransportAddress(sin);
    receiver->SetAddress(addr);

    // Update mappings
    receivers[fd] = receiver;
    fds[receiver] = fd;

    Debug("Accepting connections on TCP port %hu \n", ntohs(sin.sin_port));
}

void
TCPTransport::Register_batch(TransportReceiver *receiver,
                       const transport::Configuration &config,
                       int groupIdx, int replicaIdx)
{   //サーバ側
    Debug("replicaIdx: %d", replicaIdx);
    Debug("config.n:%d", config.n);

    UW_ASSERT(replicaIdx < config.n);
    struct sockaddr_in sin; 

    //const transport::Configuration *canonicalConfig =
    RegisterConfiguration(receiver, config, groupIdx, replicaIdx);

    // Clients don't need to accept TCP connections
    if (replicaIdx == -1) {
	return;
    }

    // Create socket
    Debug("Create Socket\n");

    int fd;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        PPanic("Failed to create socket to accept TCP connections");
    }

    // Put it in non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1)) {
        PWarning("Failed to set O_NONBLOCK");
    }

    // Set SO_REUSEADDR
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET,
                   SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set SO_REUSEADDR on TCP listening socket");
    }

    // Set TCP_NODELAY
    n = 1;
    if (setsockopt(fd, IPPROTO_TCP,
                   TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
        PWarning("Failed to set TCP_NODELAY on TCP listening socket");
    }

    n = SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_RCVBUF on socket");
    }

    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
      PWarning("Failed to set SO_SNDBUF on socket");
    }
    // Registering a replica. Bind socket to the designated
    // host/port
    const string &host = config.replica(groupIdx, replicaIdx).host;
    const string &port = config.replica(groupIdx, replicaIdx).port;

    Debug("host : %s \n", host.c_str());
    Debug("port : %s \n", port.c_str());

    BindToPort(fd, host, port);

    // Listen for connections
    if (listen(fd, 5) < 0) {
        PPanic("Failed to listen for TCP connections\n");
    }

    // Create event to accept connections
    TCPTransportTCPListener *info = new TCPTransportTCPListener();
    info->transport = this;
    info->acceptFd = fd;
    info->receiver = receiver;
    info->replicaIdx = replicaIdx;
    info->acceptEvent = event_new(libeventBase,
                                  fd,
                                  EV_READ | EV_PERSIST,
                                  TCPAcceptCallback_batch,
                                  (void *)info);
    event_add(info->acceptEvent, NULL);
    tcpListeners.push_back(info);

    // Tell the receiver its address
    socklen_t sinsize = sizeof(sin);
    if (getsockname(fd, (sockaddr *) &sin, &sinsize) < 0) {
        PPanic("Failed to get socket name");
    }
    TCPTransportAddress *addr = new TCPTransportAddress(sin);
    receiver->SetAddress(addr);

    // Update mappings
    receivers[fd] = receiver;
    fds[receiver] = fd;

    Debug("Accepting connections on TCP port %hu \n", ntohs(sin.sin_port));
}


bool TCPTransport::OrderedMulticast(TransportReceiver *src,
    const std::vector<int> &groups, const Message &m) {
  Panic("Not implemented :(.");
}



bool
TCPTransport::SendMessageInternal(TransportReceiver *src,
                                  const TCPTransportAddress &dst,
                                  const Message &m)
{   
    Debug("TCPTransport::SendMessageInternal Begin \n");
    Debug("Sending %s message over TCP to %s:%d \n",
        m.GetTypeName().c_str(), inet_ntoa(dst.addr.sin_addr),
        htons(dst.addr.sin_port));
    auto dstSrc = std::make_pair(dst, src);
    mtx.lock();
    auto kv = tcpOutgoing.find(dstSrc);
    // See if we have a connection open
    //初回だけここを通る コネクションをはる
    if (kv == tcpOutgoing.end()) {
        ConnectTCP(dstSrc);
        kv = tcpOutgoing.find(dstSrc);
    }

    struct bufferevent *ev = kv->second;
    mtx.unlock();

    UW_ASSERT(ev != NULL);

    // Serialize message
    string data;
    UW_ASSERT(m.SerializeToString(&data));
    string type = m.GetTypeName();
    size_t typeLen = type.length();
    size_t dataLen = data.length();
    size_t totalLen = (typeLen + sizeof(typeLen) +
                       dataLen + sizeof(dataLen) +
                       sizeof(totalLen) +
                       sizeof(uint32_t));

    Debug("Message is %lu total bytes \n", totalLen);

    char buf[totalLen];
    char *ptr = buf;

    // buf[0]にMAGICを代入している
    *((uint32_t *) ptr) = MAGIC;
    //ポインタが指すアドレスを変更
    ptr += sizeof(uint32_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    //buf[sizeof(uint32_t)]にtotalLenを代入
    *((size_t *) ptr) = totalLen;
    //ポインタが指すアドレスを変更
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    //buf[sizeof(uint32_t)+sizeof(size_t)]にtypeLenを代入
    *((size_t *) ptr) = typeLen;
    //ポインタが指すアドレスを変更
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-buf) < totalLen);

    UW_ASSERT((size_t)(ptr+typeLen-buf) < totalLen);
    //buf[sizeof(uint32_t)+sizeof(size_t)+sizeof(size_t)]にtypeを代入
    memcpy(ptr, type.c_str(), typeLen);
    //ポインタが指すアドレスを変更
    ptr += typeLen;
    //buf[sizeof(uint32_t)+sizeof(size_t)+sizeof(size_t)+typeLen]にdataLenを代入
    *((size_t *) ptr) = dataLen;
    //ポインタが指すアドレスを変更
    ptr += sizeof(size_t);

    UW_ASSERT((size_t)(ptr-buf) < totalLen);
    UW_ASSERT((size_t)(ptr+dataLen-buf) == totalLen);
    //buf[sizeof(uint32_t)+sizeof(size_t)+sizeof(size_t)+typeLen+sizeof(size_t)]にdataを代入
    memcpy(ptr, data.c_str(), dataLen);
    ptr += dataLen;

    //mtx.lock();
    //evbuffer_lock(ev);
    if (bufferevent_write(ev, buf, totalLen) < 0) { //evが
        Warning("Failed to write to TCP buffer");
        fprintf(stderr, "tcp write failed\n");
        //evbuffer_unlock(ev);
        //mtx.unlock();
        return false;
    }
    //evbuffer_unlock(ev);
    //mtx.unlock();

    // writeはbufferevent_writeの代わりに用いられているようだ
    // 機能としては、ファイルディスクリプタにeventを書き込む。
    /*Latency_Start(&sockWriteLat);
    if (write(ev->ev_write.ev_fd, buf, totalLen) < 0) {
      Warning("Failed to write to TCP buffer");
      return false;
    }
    Latency_End(&sockWriteLat);*/
    Debug("TCPTransport::SendMessageInternal End \n");

    return true;
}


bool
TCPTransport::SendMessageInternal_batch(TransportReceiver *src,
                                  const TCPTransportAddress &dst,
                                  const std::vector<Message *> &m_list)
{  // vectorで二重配列にした場合、内側の配列のアドレスが連続しないので、配列を二重にする。
    Debug("SendMessageInternal_batch start\n");

    int message_size = m_list.size();

    Debug("message_size : %d", message_size);

    for (int i = 0; i < message_size; i++){
        Debug("Sending %s message over TCP to %s:%d \n",
        (*m_list[i]).GetTypeName().c_str(), inet_ntoa(dst.addr.sin_addr),
        htons(dst.addr.sin_port));
    }

    auto dstSrc = std::make_pair(dst, src);
    mtx.lock();
    auto kv = tcpOutgoing.find(dstSrc);
    // See if we have a connection open
    //コネクションをはる
    if (kv == tcpOutgoing.end()) {
        ConnectTCP_batch(dstSrc);
        kv = tcpOutgoing.find(dstSrc);
    }
    struct bufferevent *ev = kv->second;
    mtx.unlock();

    UW_ASSERT(ev != NULL);

    size_t maxTotalLen = 0;
    size_t maxDataLen = 0;
    
    // maxTotalLenとmaxDataLenを調べる。
    for(int i = 0; i < message_size; i++){

        string data;
        UW_ASSERT((*m_list[i]).SerializeToString(&data));
        string type = (*m_list[i]).GetTypeName();
        size_t typeLen = type.length();
        //printf("typeLen :%d", typeLen);
        size_t dataLen = data.length();
        //printf("dataLen :%d", dataLen);
        if (dataLen > maxDataLen){
            maxDataLen = dataLen;
        }
        size_t totalLen = (typeLen + sizeof(typeLen) +
                       dataLen + sizeof(dataLen) +
                       sizeof(totalLen) +
                       sizeof(uint32_t));
       // printf("totalLen :%d", totalLen);
        if (totalLen > maxTotalLen){
            maxTotalLen = totalLen;
        }

    }

    //buf_batchの雛形を作成
    char buf_batch[message_size][maxTotalLen];

    //buf_batchの初期化
    for (int i = 0; i < message_size; i++){
        for (int j = 0; j < maxTotalLen; j++){
            buf_batch[i][j] = ' ';
        }
    }

    for(int i = 0; i < message_size; ++i){
        string data;
        UW_ASSERT((*m_list[i]).SerializeToString(&data));
        string type = (*m_list[i]).GetTypeName();
        size_t typeLen = type.length();
        size_t dataLen = maxDataLen;
        size_t totalLen = maxTotalLen;
        
        char buf[totalLen];

        // bufの中身を初期化
        for (int i = 0; i < totalLen; i++){
            buf[i] = ' ';
        }

        char *ptr = buf;

        *((uint32_t *) ptr) = MAGIC;
        ptr += sizeof(uint32_t);
        UW_ASSERT((size_t)(ptr-buf) < totalLen);

        *((size_t *) ptr) = totalLen;
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalLen);

        *((size_t *) ptr) = typeLen;
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalLen);

        UW_ASSERT((size_t)(ptr+typeLen-buf) < totalLen);
        memcpy(ptr, type.c_str(), typeLen);
        ptr += typeLen;
        *((size_t *) ptr) = dataLen;
        ptr += sizeof(size_t);

        UW_ASSERT((size_t)(ptr-buf) < totalLen);
        UW_ASSERT((size_t)(ptr+dataLen-buf) == totalLen);
        memcpy(ptr, data.c_str(), data.length());
        ptr += dataLen;

        for(int j = 0; j < totalLen; j++){
            buf_batch[i][j] = buf[j];
        }

    }

    /*
    int buf_message_size = 0;
    
    for(auto itr = totalLens.begin(); itr != totalLens.end(); ++itr){
        buf_message_size += *itr;
    }
    */


    int buf_message_size = message_size * maxTotalLen;

    Debug("buf_batch_size : %d\n", buf_message_size);

    //mtx.lock();
    //evbuffer_lock(ev);

    //二番目の項目は、配列の先頭のアドレスが入る。
    //三番目の項目は、配列の大きさが入る。
    if (bufferevent_write(ev, buf_batch, buf_message_size) < 0) { 
        Warning("Failed to write to TCP buffer");
        fprintf(stderr, "tcp write failed\n");
        //evbuffer_unlock(ev);
        //mtx.unlock();
        return false;
    }
    
    //evbuffer_unlock(ev);
    //mtx.unlock();
    //Latency_Start(&sockWriteLat);
    
    /*
    // レプリカのソケットにbuf_batchから、buf_message_sizeバイト分書き込む。
    if (write(ev->ev_write.ev_fd, buf_batch, buf_message_size) < 0) {
      Warning("Failed to write to TCP buffer");
      fprintf(stderr, "tcp write failed\n");
      return false;
    }
    */
    
    
    //Latency_End(&sockWriteLat);
    Debug("SendMessageInternal_batch end\n");
    return true;
}



// void TCPTransport::Flush() {
//   event_base_loop(libeventBase, EVLOOP_NONBLOCK);
// }

void
TCPTransport::Run()
{
    //stopped = false;
    //
    int ret = event_base_dispatch(libeventBase);
    Debug("event_base_dispatch returned %d. \n", ret);
}

void
TCPTransport::Stop()
{
  // TODO: cleaning up TCP connections needs to be done better
  // - We want to close connections from client side when we kill clients so that
  //   server doesn't see many connections in TIME_WAIT and run out of file descriptors
  // - This is mainly a problem if the client is still running long after it should have
  //   finished (due to abort loops)

 //XXX old version
  // if (!stopped) {
  //
  //   auto stopFn = [this](){
  //     if (!stopped) {
  //       stopped = true;
  //       mtx.lock();
  //       for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
  //         tcpAddresses.erase(itr->second);
  //         bufferevent_free(itr->second);
  //         itr = tcpOutgoing.erase(itr);
  //       }
  //       event_base_dump_events(libeventBase, stderr);
  //       event_base_loopbreak(libeventBase);
  //       tp.stop();
  //       //delete tp;
  //       mtx.unlock();
  //     }
  //   };
  //   if (immediately) {
  //     stopFn();
  //   } else {
  //     Timer(500, stopFn);
  //   }
  // }

  // mtx.lock();
  // for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ) {
  //   bufferevent_free(itr->second);
  //   tcpAddresses.erase(itr->second);
  //   itr = tcpOutgoing.erase(itr);
  // }

  tp.stop();
  event_base_dump_events(libeventBase, stderr);

  //mtx.unlock();
}

void TCPTransport::Close(TransportReceiver *receiver) {
  mtx.lock();
  for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ++itr) {
    if (itr->first.second == receiver) {
      bufferevent_free(itr->second);
      tcpOutgoing.erase(itr);
      tcpAddresses.erase(itr->second);
      break;
    }
  }
  mtx.unlock();
}



int TCPTransport::Timer(uint64_t ms, timer_callback_t cb) {
  struct timeval tv;
  tv.tv_sec = ms/1000;
  tv.tv_usec = (ms % 1000) * 1000;
  return TimerInternal(tv, cb);
}

int TCPTransport::TimerMicro(uint64_t us, timer_callback_t cb) {
  struct timeval tv;
  tv.tv_sec = us / 1000000UL;
  tv.tv_usec = us % 1000000UL;

  return TimerInternal(tv, cb);
}

int TCPTransport::TimerInternal(struct timeval &tv, timer_callback_t cb) {
  //mtxを使用して、ロックを行う。
  std::unique_lock<std::shared_mutex> lck(mtx);

  TCPTransportTimerInfo *info = new TCPTransportTimerInfo();

  ++lastTimerId;

  info->transport = this;
  info->id = lastTimerId;
  info->cb = cb;

  // Eventを初期化(initialized)
  info->ev = event_new(libeventBase, -1, 0, TimerCallback, info);

  timers[info->id] = info;
  
  // eventをイベントループ(eventbase)に登録
  event_add(info->ev, &tv);

  return info->id;
  //ここでアンロックされるはず。
}

bool
TCPTransport::CancelTimer(int id)
{
    std::unique_lock<std::shared_mutex> lck(mtx);
    TCPTransportTimerInfo *info = timers[id];

    if (info == NULL) {
        return false;
    }

    timers.erase(info->id);
    event_del(info->ev);
    event_free(info->ev);
    delete info;

    return true;
}

void
TCPTransport::CancelAllTimers()
{
    mtx.lock();
    while (!timers.empty()) {
        auto kv = timers.begin();
        int id = kv->first;
        mtx.unlock();
        CancelTimer(id);
        mtx.lock();
    }
    mtx.unlock();
}

void
TCPTransport::OnTimer(TCPTransportTimerInfo *info)
{
    {
	    std::unique_lock<std::shared_mutex> lck(mtx);

	    timers.erase(info->id);
	    event_del(info->ev);
	    event_free(info->ev);
    }

    info->cb();

    delete info;
}

void
TCPTransport::TimerCallback(evutil_socket_t fd, short what, void *arg)
{
    TCPTransport::TCPTransportTimerInfo *info =
        (TCPTransport::TCPTransportTimerInfo *)arg;

    UW_ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void TCPTransport::DispatchTP(std::function<void*()> f, std::function<void(void*)> cb)  {
  tp.dispatch(std::move(f), std::move(cb), libeventBase);
}

void TCPTransport::DispatchTP_local(std::function<void*()> f, std::function<void(void*)> cb)  {
  tp.dispatch_local(std::move(f), std::move(cb));
}

void TCPTransport::DispatchTP_noCB(std::function<void*()> f) {
  tp.detatch(std::move(f));
}
void TCPTransport::DispatchTP_noCB_ptr(std::function<void*()> *f) {
  tp.detatch_ptr(f);
}
void TCPTransport::DispatchTP_main(std::function<void*()> f) {
  tp.detatch_main(std::move(f));
}
void TCPTransport::IssueCB(std::function<void(void*)> cb, void* arg){
  //std::unique_lock<std::shared_mutex> lck(mtx);
  tp.issueCallback(std::move(cb), arg, libeventBase);
}

void
TCPTransport::LogCallback(int severity, const char *msg)
{
    Message_Type msgType;
    switch (severity) {
    case _EVENT_LOG_DEBUG:
        msgType = MSG_DEBUG;
        break;
    case _EVENT_LOG_MSG:
        msgType = MSG_NOTICE;
        break;
    case _EVENT_LOG_WARN:
        msgType = MSG_WARNING;
        break;
    case _EVENT_LOG_ERR:
        msgType = MSG_WARNING;
        break;
    default:
        NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void
TCPTransport::FatalCallback(int err)
{
    Panic("Fatal libevent error: %d", err);
}

void
TCPTransport::SignalCallback(evutil_socket_t fd, short what, void *arg)
{
    Debug("Terminating on SIGTERM/SIGINT");
    TCPTransport *transport = (TCPTransport *)arg;
    event_base_loopbreak(transport->libeventBase);
}

void
TCPTransport::TCPAcceptCallback(evutil_socket_t fd, short what, void *arg)
{
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;

    if (what & EV_READ) {
        int newfd;
        struct sockaddr_in sin;
        socklen_t sinLength = sizeof(sin);
        struct bufferevent *bev;

        // Accept a connection
        if ((newfd = accept(fd, (struct sockaddr *)&sin,
                            &sinLength)) < 0) {
            PWarning("Failed to accept incoming TCP connection");
            return;
        }

        // Put it in non-blocking mode
        if (fcntl(newfd, F_SETFL, O_NONBLOCK, 1)) {
            PWarning("Failed to set O_NONBLOCK");
        }

            // Set TCP_NODELAY
        int n = 1;
        if (setsockopt(newfd, IPPROTO_TCP,
                       TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
            PWarning("Failed to set TCP_NODELAY on TCP listening socket");
        }

        // Create a buffered event
        bev = bufferevent_socket_new(transport->libeventBase, newfd,
                                     BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
        bufferevent_setcb(bev, TCPReadableCallback, NULL,
                          TCPIncomingEventCallback, info);
        if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
            Panic("Failed to enable bufferevent");
        }
    info->connectionEvents.push_back(bev);
	TCPTransportAddress client = TCPTransportAddress(sin);

  transport->mtx.lock();
  auto dstSrc = std::make_pair(client, info->receiver);
	transport->tcpOutgoing[dstSrc] = bev;
	transport->tcpAddresses.insert(pair<struct bufferevent*,
        pair<TCPTransportAddress, TransportReceiver *>>(bev, dstSrc));
  transport->mtx.unlock();

    Debug("Opened incoming TCP connection from %s:%d \n",
               inet_ntoa(sin.sin_addr), htons(sin.sin_port));
    }
}

void
TCPTransport::TCPAcceptCallback_batch(evutil_socket_t fd, short what, void *arg)
{
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;

    if (what & EV_READ) {
        int newfd;
        struct sockaddr_in sin;
        socklen_t sinLength = sizeof(sin);
        struct bufferevent *bev;

        // Accept a connection
        if ((newfd = accept(fd, (struct sockaddr *)&sin,
                            &sinLength)) < 0) {
            PWarning("Failed to accept incoming TCP connection");
            return;
        }

        // Put it in non-blocking mode
        if (fcntl(newfd, F_SETFL, O_NONBLOCK, 1)) {
            PWarning("Failed to set O_NONBLOCK");
        }

            // Set TCP_NODELAY
        int n = 1;
        if (setsockopt(newfd, IPPROTO_TCP,
                       TCP_NODELAY, (char *)&n, sizeof(n)) < 0) {
            PWarning("Failed to set TCP_NODELAY on TCP listening socket");
        }

        // Create a buffered event
        bev = bufferevent_socket_new(transport->libeventBase, newfd,
                                     BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
        bufferevent_setcb(bev, TCPReadableCallback_batch, NULL,
                          TCPIncomingEventCallback, info);
        if (bufferevent_enable(bev, EV_READ|EV_WRITE) < 0) {
            Panic("Failed to enable bufferevent");
        }
    info->connectionEvents.push_back(bev);
	TCPTransportAddress client = TCPTransportAddress(sin);

  transport->mtx.lock();
  auto dstSrc = std::make_pair(client, info->receiver);
	transport->tcpOutgoing[dstSrc] = bev;
	transport->tcpAddresses.insert(pair<struct bufferevent*,
        pair<TCPTransportAddress, TransportReceiver *>>(bev, dstSrc));
  transport->mtx.unlock();

    Debug("Opened incoming TCP connection from %s:%d \n",
               inet_ntoa(sin.sin_addr), htons(sin.sin_port));
    }
}

void
TCPTransport::TCPReadableCallback(struct bufferevent *bev, void *arg)
{   
    Debug("TCPTransport::TCPReadableCallback \n");
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;
    struct evbuffer *evbuf = bufferevent_get_input(bev);

    while (evbuffer_get_length(evbuf) > 0) {
        uint32_t *magic;
        magic = (uint32_t *)evbuffer_pullup(evbuf, sizeof(*magic));
        if (magic == NULL) {
            return;
        }
        UW_ASSERT(*magic == MAGIC);

        size_t *sz;
        unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

        sz = (size_t *) (x + sizeof(*magic));
        if (x == NULL) {
            return;
        }
        size_t totalSize = *sz;
        UW_ASSERT(totalSize < 1073741826);

        if (evbuffer_get_length(evbuf) < totalSize) {
            Debug("Don't have %ld bytes for a message yet, only %ld",totalSize, evbuffer_get_length(evbuf));
            return;
        }
        // Debug("Receiving %ld byte message", totalSize);

        char buf[totalSize];
        size_t copied = evbuffer_remove(evbuf, buf, totalSize);
        UW_ASSERT(copied == totalSize);

        // Parse message
        char *ptr = buf + sizeof(*sz) + sizeof(*magic);

        size_t typeLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+typeLen-buf) < totalSize);
        string msgType(ptr, typeLen);
        ptr += typeLen;

        size_t msgLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+msgLen-buf) <= totalSize);
        string msg(ptr, msgLen);
        ptr += msgLen;

        transport->mtx.lock_shared();
        auto addr = transport->tcpAddresses.find(bev);
        TCPTransportAddress &ad = addr->second.first; //Note: if address was removed from map, ref could still be in "use" by server
        // transport->mtx.unlock();
        // UW_ASSERT(addr != transport->tcpAddresses.end());
        //
        // // Dispatch
        //
        // info->receiver->ReceiveMessage(ad, msgType, msg, nullptr);
        // Debug("Done processing large %s message", msgType.c_str());

        if (addr == transport->tcpAddresses.end()) {
         Warning("Received message for closed connection.");
         transport->mtx.unlock_shared();
       } else {
         // Dispatch //ここでメッセージを受け取っている ReceiveMessage → ReceiveMessageInternal
         transport->mtx.unlock_shared();
         Debug("Received %lu bytes %s message.\n", totalSize, msgType.c_str());
         //indicusstore/shardclient.ccのReceiveMessageを呼び出す。
         info->receiver->ReceiveMessage(ad, msgType, msg, nullptr);
         //Debug("Done processing large %s message", msgType.c_str());
       }
    }
}


void
TCPTransport::TCPReadableCallback_batch(struct bufferevent *bev, void *arg)
{   
    Debug("TCPTransport::TCPReadableCallback_batch begin\n");
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;

    //bevにリモートサーバから送られてきた、データが格納されている。
    struct evbuffer *evbuf = bufferevent_get_input(bev);

    size_t totalSize;
    std::vector<std::string> msgTypes;
    std::vector<std::string> msgs;

    int first_loop = true;

    while (evbuffer_get_length(evbuf) > 0) {
        Debug("evbuffer_get_length(evbuf) :%d \n", evbuffer_get_length(evbuf));

        //もしかしたらmagicが指す空間がヘッダーかも
        uint32_t *magic;
        // データを連続したものにする
        magic = (uint32_t *)evbuffer_pullup(evbuf, sizeof(*magic));
        if (magic == NULL) {
            Debug("magic == NULL");
            if (first_loop == true){
                return;
            }
            else {
                break;
            }
        }

        if (*magic != MAGIC){
            if (first_loop == true){
                return;
            }
            else {
                break;
            }
        }

        //szが指す領域がメッセージ
        size_t *sz;
        unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

        sz = (size_t *) (x + sizeof(*magic));
        if (x == NULL) {
            Debug("x == NULL");
            if (first_loop == true){
                return;
            }
            else {
                break;
            }
        }
        totalSize = *sz;
        if (totalSize >= 1073741826){
            if (first_loop == true){
                return;
            }
            else {
                break;
            }
        }

        if (evbuffer_get_length(evbuf) < totalSize) {
            Debug("evbuffer_get_length(evbuf) : %d\n", evbuffer_get_length(evbuf));
            Debug("totalSize: %d\n", totalSize);
            Debug("evbuffer_get_length(evbuf) < totalSize");
            Debug("Don't have %ld bytes for a message yet, only %ld", totalSize, evbuffer_get_length(evbuf));
            if (first_loop == true){
                return;
            }
            else {
                break;
            }   
        }
        // Debug("Receiving %ld byte message", totalSize);
        char buf[totalSize];
        size_t copied = evbuffer_remove(evbuf, buf, totalSize);
        UW_ASSERT(copied == totalSize);

        // Parse message
        char *ptr = buf + sizeof(*sz) + sizeof(*magic);

        size_t typeLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+typeLen-buf) < totalSize);
        string messageType(ptr, typeLen);
        ptr += typeLen;

        size_t msgLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        UW_ASSERT((size_t)(ptr-buf) < totalSize);

        UW_ASSERT((size_t)(ptr+msgLen-buf) <= totalSize);
        string msg(ptr, msgLen);
        ptr += msgLen;

        msgTypes.push_back(messageType);
        msgs.push_back(msg);
        first_loop = false;
    }
    
    transport->mtx.lock_shared();
    auto addr = transport->tcpAddresses.find(bev);
    TCPTransportAddress &ad = addr->second.first; 
    if (addr == transport->tcpAddresses.end()) {
         Warning("Received message for closed connection.");
         transport->mtx.unlock_shared();
    } else {
         transport->mtx.unlock_shared();
         Debug("Received %lu bytes %s message.\n", totalSize, msgTypes[0].c_str());
         info->receiver->ReceiveMessage_batch(ad, msgTypes, msgs, nullptr);
         Debug("Done processing large %s message", msgTypes[0].c_str());
    }
}


/*
void
TCPTransport::TCPReadableCallback_batch(struct bufferevent *bev, void *arg)
{   
    printf("TCPTransport::TCPReadableCallback_batch begin\n");
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;
    struct evbuffer *evbuf = bufferevent_get_input(bev);

    //bufferevent_read_buffer(bev, evbuf);

    bool two_type = false;
    std::vector<std::string> test_msgTypes;

    size_t totalSize;
    std::string msgType;
    std::vector<std::string> msgTypes;
    std::vector<std::string> msgs;

    size_t typeOneSize = 0;
    int loop_count = 0;

    struct evbuffer_ptr evbuf_ptr;
    if (evbuffer_ptr_set(evbuf, &evbuf_ptr, 0, EVBUFFER_PTR_SET) != 0){
      printf("Failed to set evbuffer_ptr");
    }

    size_t evbuffer_length = evbuffer_get_length(evbuf);

    while (evbuffer_length > 60) {
        printf("evbuffer_length:%d\n", evbuffer_length);
        uint32_t *magic;
        magic = (uint32_t *)evbuffer_pullup(evbuf, sizeof(*magic));
        if (magic == NULL) {
            printf("magic == NULL");
            return;
        }
        if (*magic != MAGIC){
            printf("*magic == MAGIC");
            return;
        }

        size_t *sz;
        unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

        sz = (size_t *) (x + sizeof(*magic));
        if (x == NULL) {
            printf("x == NULL");
            return;
        }
        totalSize = *sz;
        if (totalSize >= 1073741826){
            printf("totalSize >= 1073741826");
            return;
        }

        // printf("Receiving %ld byte message", totalSize);
        char buf[totalSize];
        size_t copied = evbuffer_copyout_from(evbuf, &evbuf_ptr, buf, totalSize);
        //UW_ASSERT(copied == totalSize);
        evbuffer_length -= copied;

        // Parse message
        char *ptr = buf + sizeof(*sz) + sizeof(*magic);

        size_t typeLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        //UW_ASSERT((size_t)(ptr-buf) < totalSize);


        //UW_ASSERT((size_t)(ptr+typeLen-buf) < totalSize);
        string messageType(ptr, typeLen);
        msgType = messageType;
        ptr += typeLen;

        printf("msgType, %s\n", msgType.c_str());

        size_t msgLen = *((size_t *)ptr);
        ptr += sizeof(size_t);
        //UW_ASSERT((size_t)(ptr-buf) < totalSize);

        //UW_ASSERT((size_t)(ptr+msgLen-buf) <= totalSize);
        string msg(ptr, msgLen);
        ptr += msgLen;
        
        if (loop_count != 0 && msgTypes[0] != msgType){
            two_type = true;
            printf("there are two type \n");
            printf("msgTypes[0]: %s \n", msgTypes[0].c_str());
            printf("msgType: %s \n", msgType.c_str());
            char buf[typeOneSize];
            evbuffer_remove(evbuf, buf, typeOneSize);
            printf("typeOneSize: %d \n", typeOneSize);
            break;
        }

        msgTypes.push_back(msgType);
        msgs.push_back(msg);

        if (evbuffer_ptr_set(evbuf, &evbuf_ptr, totalSize, EVBUFFER_PTR_ADD) != 0){
            printf("Failed to add evbuffer_ptr");
        }

        typeOneSize += totalSize;
        
        loop_count++;
    }

    //evbufferに完璧なデータが入っていなかったら、evbufferには何の修正も入れずにreturnする
    if (evbuffer_get_length(evbuf) % totalSize != 0 && two_type == false){
        printf("evbuffer_get_length(evbuf) % totalSize != 0 && two_type == false\n");
        return;
    }

    if (two_type == true){
        while(evbuffer_get_length(evbuf) > 0){
            uint32_t *magic;
            magic = (uint32_t *)evbuffer_pullup(evbuf, sizeof(*magic));
            if (magic == NULL) {
                printf("magic == NULL");
                break;
            }
            if (*magic != MAGIC){
                printf("*magic == MAGIC");
                return;
            }

            size_t *sz;
            unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

            sz = (size_t *) (x + sizeof(*magic));
            if (x == NULL) {
                printf("x == NULL");
                break;
            }
            totalSize = *sz;
            if (totalSize >= 1073741826){
                printf("totalSize >= 1073741826");
                return;
            }

            //ここの部分はコメントアウトする
            if (evbuffer_get_length(evbuf) % totalSize != 0){
                printf("evbuffer_get_length(evbuf) percentage totalSize != 0");
                break;
            }
            

            if (evbuffer_get_length(evbuf) < totalSize) {
                printf("evbuffer_get_length(evbuf) : %d\n", evbuffer_get_length(evbuf));
                printf("totalSize: %d\n", totalSize);
                printf("evbuffer_get_length(evbuf) < totalSize");
                //printf("Don't have %ld bytes for a message yet, only %ld",
                //      totalSize, evbuffer_get_length(evbuf));
                return;
            }
            // printf("Receiving %ld byte message", totalSize);
            char buf[totalSize];
            size_t copied = evbuffer_remove(evbuf, buf, totalSize);
            UW_ASSERT(copied == totalSize);
            //UW_ASSERT(copied == totalSize);

            // Parse message
            char *ptr = buf + sizeof(*sz) + sizeof(*magic);

            size_t typeLen = *((size_t *)ptr);
            ptr += sizeof(size_t);
            UW_ASSERT((size_t)(ptr-buf) < totalSize);

            UW_ASSERT((size_t)(ptr+typeLen-buf) < totalSize);
            string messageType(ptr, typeLen);
            msgType = messageType;
            ptr += typeLen;

            size_t msgLen = *((size_t *)ptr);
            ptr += sizeof(size_t);
            UW_ASSERT((size_t)(ptr-buf) < totalSize);

            UW_ASSERT((size_t)(ptr+msgLen-buf) <= totalSize);
            string msg(ptr, msgLen);
            ptr += msgLen;

            msgTypes.push_back(msgType);
            msgs.push_back(msg);
        }
    }
    else {
        size_t evbuffer_length = evbuffer_get_length(evbuf);
        char buf[evbuffer_length];
        size_t copied = evbuffer_remove(evbuf, buf, evbuffer_length);
        UW_ASSERT(copied == evbuffer_length);
    }
    
    
    transport->mtx.lock_shared();
    auto addr = transport->tcpAddresses.find(bev);
    TCPTransportAddress &ad = addr->second.first; 
    if (addr == transport->tcpAddresses.end()) {
         Warning("Received message for closed connection.");
         transport->mtx.unlock_shared();
    } else {
         // Dispatch //ここでメッセージを受け取っている ReceiveMessage → ReceiveMessageInternal
         transport->mtx.unlock_shared();
         printf("Received %lu bytes %s message.\n", totalSize, msgType.c_str());
         //ここがserver->ReceiveMessageを呼び出している。
         info->receiver->ReceiveMessage_batch(ad, msgTypes, msgs, nullptr);
         // printf("Done processing large %s message", msgType.c_str());
    }
}
*/



void
TCPTransport::TCPIncomingEventCallback(struct bufferevent *bev,
                                       short what, void *arg)
{
    if (what & BEV_EVENT_ERROR) {
      fprintf(stderr,"tcp incoming error\n");
        Debug("Error on incoming TCP connection: %s\n",
                evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        bufferevent_free(bev);
        return;
    } else if (what & BEV_EVENT_ERROR) {
      fprintf(stderr,"tcp incoming eof\n");
        Debug("EOF on incoming TCP connection\n");
        bufferevent_free(bev);
        return;
    }
}

//Note: If ever to make client multithreaded, add mutexes here. (same for ConnectTCP)
void
TCPTransport::TCPOutgoingEventCallback(struct bufferevent *bev,
                                       short what, void *arg)
{   
    Debug("TCPTransport::TCPOutgoingEventCallback \n");
    TCPTransportTCPListener *info = (TCPTransportTCPListener *)arg;
    TCPTransport *transport = info->transport;
    transport->mtx.lock_shared();
    auto it = transport->tcpAddresses.find(bev);

    UW_ASSERT(it != transport->tcpAddresses.end());
    TCPTransportAddress addr = it->second.first;
    transport->mtx.unlock_shared();

    if (what & BEV_EVENT_CONNECTED) {
        Debug("Established outgoing TCP connection to server [g:%d][r:%d] \n", info->groupIdx, info->replicaIdx);
    } else if (what & BEV_EVENT_ERROR) {
        Warning("Error on outgoing TCP connection to server [g:%d][r:%d]: %s \n",
                info->groupIdx, info->replicaIdx,
                evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        bufferevent_free(bev);

        transport->mtx.lock();
        auto it2 = transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
        transport->tcpOutgoing.erase(it2);
        transport->tcpAddresses.erase(bev);
        transport->mtx.unlock();

        return;
    } else if (what & BEV_EVENT_EOF) {
        //Warning("EOF on outgoing TCP connection to server");
        bufferevent_free(bev);

        transport->mtx.lock();
        auto it2 = transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
        transport->tcpOutgoing.erase(it2);
        transport->tcpAddresses.erase(bev);
        transport->mtx.unlock();

        return;
    }
}
