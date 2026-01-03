#include "process.h"
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;

void handleClient(int clientFd) {
    struct timeval tv;
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    setsockopt(clientFd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

    char buf[8192];
    while (true) {
        ssize_t n = recv(clientFd, buf, sizeof(buf) - 1, 0);
        if (n <= 0)
            break;

        buf[n] = '\0';
        string query(buf);
        string result;
        
        try {
            result = processQuery(query);
        } catch (const std::exception& e) {
            result = "Error: " + string(e.what());
        } catch (...) {
            result = "Error: unknown error";
        }

        if (result.empty()) {
            result = "OK";
        }

        if (result.back() != '\n') {
            result += "\n";
        }
        
        result += "\x04";

        ssize_t sent = send(clientFd, result.c_str(), result.size(), MSG_NOSIGNAL);
        if (sent == -1) {
            break;
        }
    }
    close(clientFd);
}

int startServer() {
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd == -1) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(7432);

    if (bind(serverFd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(serverFd);
        return 1;
    }

    if (listen(serverFd, 1000) == -1) {
        perror("listen");
        close(serverFd);
        return 1;
    }

    cout << "Listen on 7432 (Multi-threaded Blocking Mode)\n";

    while (true) {
        int clientFd = accept(serverFd, nullptr, nullptr);
        if (clientFd == -1)
            continue;

        thread(handleClient, clientFd).detach();
    }

    close(serverFd);
    return 0;
}

int startClient(const string& host, const string& schema, const string& query) {
    string command = "-s \"" + schema + "\" -q \"" + query + "\"";
    struct addrinfo hints = {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* result = nullptr;
    if (getaddrinfo(host.c_str(), "7432", &hints, &result) != 0)
        return 1;
    int sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (sockfd == -1)
        return 1;
    if (connect(sockfd, result->ai_addr, result->ai_addrlen) != 0) {
        close(sockfd);
        return 1;
    }
    freeaddrinfo(result);
    command += '\n';
    send(sockfd, command.c_str(), command.size(), 0);
    
    char buf[4096];
    string response;
    while (true) {
        ssize_t n = recv(sockfd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) break;
        buf[n] = '\0';
        string chunk(buf);
        size_t pos = chunk.find('\x04');
        if (pos != string::npos) {
            response += chunk.substr(0, pos);
            break;
        } else {
            response += chunk;
        }
    }
    cout << response;
    
    close(sockfd);
    return 0;
}

int main(int argc, char* argv[]) {
    string host, schema, query;
    bool serverMode = false;
    int opt;
    while ((opt = getopt(argc, argv, "c:s:q:l")) != -1) {
        switch (opt) {
        case 'c':
            host = optarg;
            break;
        case 's':
            schema = optarg;
            break;
        case 'q':
            query = optarg;
            break;
        case 'l':
            serverMode = true;
            break;
        default:
            return 1;
        }
    }
    if (serverMode)
        return startServer();
    if (host.empty() || schema.empty() || query.empty())
        return 1;
    return startClient(host, schema, query);
}