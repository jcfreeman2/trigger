/**
 * @file TokenManager.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "trigger/TokenManager.hpp"
#include "trigger/LivetimeCounter.hpp"

#include "networkmanager/NetworkManager.hpp"

#include <memory>
#include <string>

namespace dunedaq::trigger {

TokenManager::TokenManager(const std::string& connection_name,
                           int initial_tokens,
                           daqdataformats::run_number_t run_number,
                           std::shared_ptr<LivetimeCounter> livetime_counter)
  : m_n_tokens(initial_tokens)
  , m_connection_name(connection_name)
  , m_run_number(run_number)
  , m_livetime_counter(livetime_counter)

{

  m_open_trigger_time = std::chrono::steady_clock::now();

  networkmanager::NetworkManager::get().start_listening(m_connection_name);

  networkmanager::NetworkManager::get().register_callback(
    m_connection_name, std::bind(&TokenManager::receive_token, this, std::placeholders::_1));
}

TokenManager::~TokenManager()
{

  networkmanager::NetworkManager::get().clear_callback(m_connection_name);

  networkmanager::NetworkManager::get().stop_listening(m_connection_name);

  if (!m_open_trigger_decisions.empty()) {

    auto now = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::milliseconds>(now - m_open_trigger_time) >
        std::chrono::milliseconds(3000)) {
      std::ostringstream o;
      o << "Open Trigger Decisions: [";
      { // Scope for lock_guard
        bool first = true;
        std::lock_guard<std::mutex> lk(m_open_trigger_decisions_mutex);
        for (auto& td : m_open_trigger_decisions) {
          if (!first)
            o << ", ";
          o << td;
          first = false;
        }
        o << "]";
      }
      TLOG_DEBUG(0) << o.str();
    }
  }
}

int
TokenManager::get_n_tokens() const
{
  return m_n_tokens.load();
}

void
TokenManager::trigger_sent(dfmessages::trigger_number_t trigger_number)
{
  std::lock_guard<std::mutex> lk(m_open_trigger_decisions_mutex);
  m_open_trigger_decisions.insert(trigger_number);
  m_n_tokens--;
  if(m_n_tokens.load()==0) {
    m_livetime_counter->set_state(LivetimeCounter::State::kDead);
  }
}

void
TokenManager::receive_token(ipm::Receiver::Response message)
{

  auto token = serialization::deserialize<dfmessages::TriggerDecisionToken>(message.data);

  TLOG_DEBUG(1) << "Received token with run number " << token.run_number << ", current run number " << m_run_number;
  if (token.run_number == m_run_number) {
    if(m_n_tokens.load()==0){
      m_livetime_counter->set_state(LivetimeCounter::State::kLive);
    }
    m_n_tokens++;
    TLOG_DEBUG(1) << "There are now " << m_n_tokens.load() << " tokens available";

    if (token.trigger_number != dfmessages::TypeDefaults::s_invalid_trigger_number) {
      if (m_open_trigger_decisions.count(token.trigger_number)) {
        std::lock_guard<std::mutex> lk(m_open_trigger_decisions_mutex);
        m_open_trigger_decisions.erase(token.trigger_number);
        TLOG_DEBUG(1) << "Token indicates that trigger decision " << token.trigger_number
                      << " has been completed. There are now " << m_open_trigger_decisions.size()
                      << " triggers in flight";
      } else {
        // ERS warning: received token for trigger number I don't recognize
      }
    }
  }
}

} // namespace dunedaq::trigger
