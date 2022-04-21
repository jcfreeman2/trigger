#include "trigger/LivetimeCounter.hpp"

#include "logging/Logging.hpp"

#include <sstream>

namespace dunedaq::trigger {

LivetimeCounter::LivetimeCounter(LivetimeCounter::State state)
  : m_state(state)
  , m_last_state_change_time(now())
{
  TLOG_DEBUG(1) << "Starting LivetimeCounter in state " << get_state_name(state);
  m_state_times[State::kLive]=0;
  m_state_times[State::kDead]=0;
  m_state_times[State::kPaused]=0;
}

LivetimeCounter::~LivetimeCounter()
{
  std::string report=get_report_string();
  TLOG() << "LivetimeCounter stopping. Counts: " << report;
}

void
LivetimeCounter::update_map()
{
  // Caller must lock the mutex, so we don't here
  auto current_time=now();
  auto delta=(current_time-m_last_state_change_time);
  m_state_times[m_state]+=delta;
  m_last_state_change_time=current_time;
}


void
LivetimeCounter::set_state(LivetimeCounter::State state)
{
  std::lock_guard<std::mutex> l(m_mutex);
  update_map(); // Add the time to the old state
  TLOG_DEBUG(1) << "Changing state from " << get_state_name(m_state) << " to " << get_state_name(state);
  m_state=state;
}

std::map<LivetimeCounter::State, LivetimeCounter::state_time_t>
LivetimeCounter::get_time_map()
{
  std::lock_guard<std::mutex> l(m_mutex);
  update_map();
  return m_state_times;
}

LivetimeCounter::state_time_t
LivetimeCounter::get_time(LivetimeCounter::State state)
{
  std::lock_guard<std::mutex> l(m_mutex);
  update_map();
  return m_state_times[state];
}

LivetimeCounter::state_time_t
LivetimeCounter::now() const
{
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

std::string
LivetimeCounter::get_report_string()
{
  std::lock_guard<std::mutex> l(m_mutex);
  update_map();
  std::ostringstream oss;
  for(auto const& [state, t]: m_state_times){
    oss << get_state_name(state) << ": " << t << "ms ";
  }
  return oss.str();
}


std::string
LivetimeCounter::get_state_name(LivetimeCounter::State state) const
{
  switch(state){
  case State::kLive: return "live";
  case State::kDead: return "dead";
  case State::kPaused: return "paused";
  default:
    return "UNKNOWN";
  }
}

} // namespace dunedaq::trigger
