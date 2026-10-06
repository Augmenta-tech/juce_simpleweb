#include <string>
#include "../common/WSCrypto.h"
#include "../websocket/server_ws.hpp"
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <iostream>
#include <stdexcept>

#define CHECK(x) do { if(!(x)) throw std::runtime_error("CHECK failed: " #x); } while(false)
namespace asio = boost::asio;
using Tcp = asio::ip::tcp;
using Server = SimpleWeb::SocketServer<SimpleWeb::WS>;
using Error = SimpleWeb::error_code;

// Compile the shared send/close implementation for TLS too (not a TLS runtime test).
using TLSConnection = SimpleWeb::SocketServerBase<asio::ssl::stream<Tcp::socket>>::Connection;
void compile_tls_sends(const std::shared_ptr<TLSConnection> &connection) {
  connection->send("reliable");
  connection->send_latest("snapshot");
  connection->get_send_queue_stats();
}

struct Fixture {
  std::shared_ptr<asio::io_context> io = std::make_shared<asio::io_context>();
  Server server;
  Tcp::acceptor acceptor{*io, Tcp::endpoint(Tcp::v4(), 0)};
  std::vector<std::unique_ptr<Tcp::socket>> peers;

  Fixture() {
    server.io_service = io;
    server.config.max_send_queue_bytes = 1024 * 1024;
    server.config.max_send_queue_messages = 256;
  }
  std::shared_ptr<Server::Connection> connect(const std::string &path) {
    auto peer = std::make_unique<Tcp::socket>(*io);
    peer->connect(acceptor.local_endpoint());
    auto socket = std::make_unique<Tcp::socket>(*io);
    acceptor.accept(*socket);
    auto connection = std::make_shared<Server::Connection>(std::move(socket));
    connection->path = path;
    connection->header.emplace("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
    peers.push_back(std::move(peer));
    server.upgrade(connection); // Exercise the path used by the JUCE HTTP wrapper.
    return connection;
  }
  template <typename Predicate>
  void run_until(Predicate done) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while(!done()) {
      if(std::chrono::steady_clock::now() >= deadline)
        throw std::runtime_error("Timed out waiting for WebSocket completion");
      io->restart();
      io->run_for(std::chrono::milliseconds(10));
    }
  }
  ~Fixture() {
    // Do not invoke application callbacks after their captured locals expire.
    for(auto &entry : server.endpoint) {
      entry.second.on_open = nullptr;
      entry.second.on_error = nullptr;
      entry.second.on_close = nullptr;
    }
    for(auto &peer : peers) {
      Error ignored;
      peer->close(ignored);
    }
    io->restart();
    io->run_for(std::chrono::milliseconds(150));
    server.stop();
  }
};

int main() {
  try {
    {
      Fixture f;
      bool opened = false;
      auto &endpoint = f.server.endpoint["^/latest$"];
      endpoint.on_open = [&](auto connection) {
        opened = true;
        // Do not yield to the I/O completion handler: simulate a blocked writer.
        for(int i = 0; i < 10000; ++i)
          connection->send_latest(std::string(4096, 'x'));
        auto stats = connection->get_send_queue_stats();
        CHECK(stats.messages == 2 && stats.bytes == 2 * (4096 + 4));
        CHECK(stats.replaced == 9998 && !stats.stopped);
      };
      auto connection = f.connect("/latest");
      // The producer loop itself can exceed 150 ms under ASan. Wait for the
      // completion handlers too, rather than assuming a fixed run_for drained it.
      f.run_until([&] { return opened && connection->get_send_queue_stats().bytes == 0; });
      CHECK(opened);
      CHECK(connection->get_send_queue_stats().bytes == 0);
    }
    {
      Fixture f;
      f.server.config.max_send_queue_bytes = 100;
      int callbacks = 0, errors = 0, nested = 0;
      bool healthy_sent = false;
      const auto overflow = SimpleWeb::make_error_code::make_error_code(SimpleWeb::errc::no_buffer_space);
      auto &slow = f.server.endpoint["^/slow$"];
      slow.on_open = [&](auto connection) {
        auto callback = [&](const Error &ec) { CHECK(ec == overflow); ++callbacks; };
        connection->send(std::string(60, 'a'), callback); // 62 bytes in flight.
        connection->send(std::string(60, 'b'), callback); // Reject instead of exceeding 100.
        connection->send("again", [&, connection](const Error &ec) {
          CHECK(ec == overflow);
          ++callbacks;
          // This must not deadlock by invoking callbacks under send_queue_mutex.
          connection->send("nested", [&](const Error &nested_ec) {
            CHECK(nested_ec == overflow);
            ++nested;
          });
        });
        auto stats = connection->get_send_queue_stats();
        CHECK(stats.stopped && stats.bytes == 62 && stats.messages == 1);
      };
      slow.on_error = [&](auto, const Error &ec) { CHECK(ec == overflow); ++errors; };
      auto &healthy = f.server.endpoint["^/healthy$"];
      healthy.on_open = [&](auto connection) {
        connection->send("live", [&](const Error &ec) { CHECK(!ec); healthy_sent = true; });
      };
      auto overloaded = f.connect("/slow");
      auto good = f.connect("/healthy");
      f.run_until([&] {
        return callbacks == 3 && nested == 1 && errors == 1 && healthy_sent
            && overloaded->get_send_queue_stats().bytes == 0;
      });
      CHECK(callbacks == 3 && nested == 1 && errors == 1);
      CHECK(healthy_sent && !good->get_send_queue_stats().stopped);
      CHECK(overloaded->get_send_queue_stats().bytes == 0);
      CHECK(f.server.get_connections().size() == 1);
    }
    std::cout << "PASS: WS HTTP upgrade, 10000 latest sends, queue drain, overload disconnect, "
                 "callback reentry, endpoint cleanup, healthy second client; TLS send template compiled\n";
  }
  catch(const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
