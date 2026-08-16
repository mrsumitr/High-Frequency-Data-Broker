#pragma once

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

//opens a raw TCP listen socket on the givern port. NO HTTP, no
// Websocket framing - just accept() and read() straight off the wire.
class TcpListener {
public:
  explicit TcpListener(uint16_t port){
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if(listen_fd_ < 0){
      std::perror("Socket");
      std::exit(1);
    }
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET,SO_REUSEADDR, &opt, sizeof(opt));
    

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr))<0){
      std::perror("bind");
      std::exit(1);
    }

    if (listen(listen_fd_, /*backlog=*/16)< 0){
      std::perror("listen");
      std::exit(1);
    }
    std::printf("TCP listener bound on port %u, waiting for a connection... \n", port);
  }

  ~TcpListener(){
    if (listen_fd_ >=0) close(listen_fd_);
  }

  TcpListener(const TcpListener&) =delete;
  TcpListener& operator = (const TcpListener&) = delete;

  //Block until a client connects. Returns the connected socket fd, or
  //-1 if the blocking accept() was interrupted by a signal (e.g. the
  //Ctrl+C shutdown handler) -- the caller should check the shutdown
  //flag rather than treating -1 as a fatal error.
  int accept_connectivity(){
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);

    if(client_fd < 0){
      if (errno == EINTR) {
        return -1;
      }
      std::perror("accept");
      std::exit(1);
    }
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, ip_str,sizeof(ip_str));
    std::printf("Client connected from %s:%u\n", ip_str, ntohs(client_addr.sin_port));
    return client_fd;
  }
private:
    int listen_fd_ = -1;

};

