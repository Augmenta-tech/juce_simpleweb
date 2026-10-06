#ifndef SIMPLE_WEB_SEND_QUEUE_HPP
#define SIMPLE_WEB_SEND_QUEUE_HPP

#include <cstddef>
#include <list>
#include <utility>

namespace SimpleWeb {
  // Caller serializes access. Message supplies size() and bool replaceable.
  // The front may be owned by async_write: never replace or discard it early.
  template <class Message>
  class SendQueue {
  public:
    enum class Result { accepted, overflow, stopped };
    static constexpr std::size_t default_max_bytes = 64 * 1024 * 1024;
    static constexpr std::size_t default_max_messages = 256;

    struct Stats {
      std::size_t bytes, messages, replaced;
      bool stopped;
    };

    explicit SendQueue(std::size_t max_bytes = default_max_bytes,
                       std::size_t max_messages = default_max_messages)
        : max_bytes_(max_bytes), max_messages_(max_messages) {}

    // Configure before the first send (including HTTP upgrade connections).
    void set_limits(std::size_t max_bytes, std::size_t max_messages) {
      max_bytes_ = max_bytes;
      max_messages_ = max_messages;
    }

    Result push(Message message) {
      if(stopped_)
        return Result::stopped;

      // Reliable sends invalidate older snapshots too, preserving causal order.
      if(!messages_.empty()) {
        auto it = messages_.begin();
        ++it;
        while(it != messages_.end()) {
          if(it->replaceable) {
            bytes_ -= it->size();
            it = messages_.erase(it);
            ++replaced_;
          }
          else
            ++it;
        }
      }

      const auto size = message.size();
      // Subtraction avoids overflow. Headers and the in-flight message count.
      if(messages_.size() >= max_messages_ || size > max_bytes_ || bytes_ > max_bytes_ - size) {
        stopped_ = true;
        return Result::overflow;
      }
      messages_.push_back(std::move(message));
      bytes_ += size;
      return Result::accepted;
    }

    Message &front() { return messages_.front(); }
    bool empty() const { return messages_.empty(); }
    std::size_t size() const { return messages_.size(); }
    Stats stats() const { return {bytes_, messages_.size(), replaced_, stopped_}; }

    // Only call once the async operation using the front has completed.
    void pop_front() {
      bytes_ -= messages_.front().size();
      messages_.pop_front();
    }

    void stop() { stopped_ = true; }

    // Likewise, the caller must wait for async completion before draining.
    std::list<Message> take_all() {
      std::list<Message> result;
      result.swap(messages_);
      bytes_ = 0;
      return result;
    }

  private:
    std::list<Message> messages_;
    std::size_t bytes_ = 0, replaced_ = 0;
    std::size_t max_bytes_, max_messages_;
    bool stopped_ = false;
  };
} // namespace SimpleWeb

#endif
