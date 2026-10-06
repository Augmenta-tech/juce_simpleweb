#include "../websocket/send_queue.hpp"
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

#define CHECK(x) do { if(!(x)) throw std::runtime_error("CHECK failed: " #x); } while(false)

struct Message {
  std::size_t bytes;
  int id;
  bool replaceable;
  std::shared_ptr<int> lifetime;
  std::size_t size() const { return bytes; }
};
using Queue = SimpleWeb::SendQueue<Message>;
using Result = Queue::Result;

int main() {
  try {
    // A stalled socket must retain its in-flight buffer and only the newest snapshot.
    Queue snapshots(1024, 256);
    auto first = std::make_shared<int>(0);
    std::weak_ptr<int> first_weak = first;
    CHECK(snapshots.push({10, 0, true, first}) == Result::accepted);
    first.reset();
    std::weak_ptr<int> previous;
    for(int i = 1; i <= 100000; ++i) {
      auto payload = std::make_shared<int>(i);
      CHECK(snapshots.push({static_cast<std::size_t>(20 + i % 10), i, true, payload}) == Result::accepted);
      CHECK(previous.expired());
      CHECK(!first_weak.expired());
      previous = payload;
      CHECK(snapshots.stats().messages == 2);
      CHECK(snapshots.stats().bytes == static_cast<std::size_t>(30 + i % 10));
    }
    CHECK(snapshots.stats().replaced == 99999);
    CHECK(snapshots.front().id == 0);
    snapshots.pop_front();
    CHECK(first_weak.expired());
    CHECK(snapshots.front().id == 100000);
    snapshots.pop_front();
    CHECK(previous.expired());
    CHECK(snapshots.empty() && snapshots.stats().bytes == 0);

    // Reliable events retain order; newer reliable state invalidates stale snapshots.
    Queue ordered(1000, 10);
    CHECK(ordered.push({10, 1, false, {}}) == Result::accepted);
    CHECK(ordered.push({20, 2, true, {}}) == Result::accepted);
    CHECK(ordered.push({30, 3, false, {}}) == Result::accepted);
    CHECK(ordered.stats().bytes == 40 && ordered.size() == 2);
    CHECK(ordered.push({40, 4, true, {}}) == Result::accepted);
    CHECK(ordered.push({50, 5, true, {}}) == Result::accepted);
    CHECK(ordered.front().id == 1);
    ordered.pop_front();
    CHECK(ordered.front().id == 3);
    ordered.pop_front();
    CHECK(ordered.front().id == 5 && ordered.stats().bytes == 50);

    // Byte limit includes the in-flight message. Overflow is terminal.
    Queue bytes(100, 256);
    auto active = std::make_shared<int>(1);
    std::weak_ptr<int> active_weak = active;
    CHECK(bytes.push({60, 1, false, active}) == Result::accepted);
    active.reset();
    CHECK(bytes.push({40, 2, false, {}}) == Result::accepted);
    CHECK(bytes.push({1, 3, false, {}}) == Result::overflow);
    CHECK(!active_weak.expired());
    CHECK(bytes.stats().bytes == 100 && bytes.stats().stopped);
    for(int i = 0; i < 100000; ++i)
      CHECK(bytes.push({1, 4, false, {}}) == Result::stopped);
    CHECK(bytes.stats().bytes == 100 && bytes.size() == 2);
    auto failed = bytes.take_all();
    CHECK(bytes.empty() && bytes.stats().bytes == 0 && failed.size() == 2);
    failed.clear();
    CHECK(active_weak.expired());
    CHECK(bytes.push({1, 5, false, {}}) == Result::stopped);

    // Message-count limit also bounds tiny/control frames.
    Queue messages(1000000, 3);
    for(int i = 0; i < 3; ++i)
      CHECK(messages.push({0, i, false, {}}) == Result::accepted);
    CHECK(messages.push({0, 4, false, {}}) == Result::overflow);
    CHECK(messages.size() == 3);

    Queue oversized(100, 10);
    CHECK(oversized.push({101, 1, false, {}}) == Result::overflow);
    CHECK(oversized.empty());
    Queue zero_bytes(0, 10), zero_count(100, 0);
    CHECK(zero_bytes.push({1, 1, false, {}}) == Result::overflow);
    CHECK(zero_count.push({1, 1, false, {}}) == Result::overflow);

    // Ensure checked subtraction cannot wrap around size_t.
    const auto maximum = (std::numeric_limits<std::size_t>::max)();
    Queue arithmetic(maximum, 10);
    CHECK(arithmetic.push({maximum - 1, 1, false, {}}) == Result::accepted);
    CHECK(arithmetic.push({2, 2, false, {}}) == Result::overflow);
    CHECK(arithmetic.stats().bytes == maximum - 1);

    // Completing sends frees budget; stopping does not free in-flight buffers.
    Queue drain(10, 2);
    CHECK(drain.push({10, 1, false, {}}) == Result::accepted);
    drain.pop_front();
    CHECK(drain.push({10, 2, false, {}}) == Result::accepted);
    drain.stop();
    CHECK(drain.stats().bytes == 10);
    CHECK(drain.push({1, 3, true, {}}) == Result::stopped);

    // An overloaded client cannot consume the budget of a healthy connection.
    Queue healthy(100, 3);
    CHECK(healthy.push({20, 1, false, {}}) == Result::accepted);
    CHECK(!healthy.stats().stopped);
    CHECK(bytes.stats().stopped);
    std::cout << "PASS: latest replacement (100000 frames), payload lifetime, reliable ordering, "
                 "byte/count limits, terminal overload (100000 retries), overflow arithmetic, "
                 "drain accounting, stop, and independent clients\n";
  }
  catch(const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
