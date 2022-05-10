#ifndef TRIGGER_PLUGINS_LIVETIMECOUNTER_HPP_
#define TRIGGER_PLUGINS_LIVETIMECOUNTER_HPP_
#include <chrono>
#include <map>
#include <mutex>
#include <string>

namespace dunedaq::trigger {

/**
 ** @brief LivetimeCounter counts the total time in milliseconds spent in each of the available states
 **
 ** The current state is set at construction, and can be changed with
 ** set_state(). The accumulated time in a particular state can be
 ** retrieved with get_time(State), and the times spent in all the
 ** states with get_time_map()
 **/
class LivetimeCounter
{
public:
  enum class State {
    kLive,  // Live to triggers
    kDead,  // Dead to triggers due to a problem
    kPaused // Triggers paused (so we are dead to triggers, but intentionally)
  };

 /**
 ** @brief A type to store a time duration in milliseconds
 **/
  using state_time_t = uint64_t;


 /**
 ** @brief Construct a LivetimeCounter in the given state
 **
 ** Counting the time in the initial state begins immediately
 **/
  LivetimeCounter(State state);

  ~LivetimeCounter();


 /**
 ** @brief Set the current state to @a state
 **
 ** Updates the accumulated time in the previous state and switches the state
 **/
  void set_state(State state);

 /**
 ** @brief Get a map of accumulated time in milliseconds in each state
 **/
  std::map<State, state_time_t> get_time_map();

 /**
 ** @brief Get the accumulated time in milliseconds spent in a particular state
 **/
  state_time_t get_time(State state);

 /**
 ** @brief Get a nicely-formatted string of the time spent in each state
 **/
  std::string get_report_string();

  std::string get_state_name(State state) const;
  
private:
  state_time_t now() const;
  // Precondition:  m_mutex is locked by caller
     void update_map();
       
     std::mutex m_mutex;
     State m_state;
     std::map<State, state_time_t> m_state_times;
     state_time_t m_last_state_change_time;
  
};
} // namespace dunedaq::trigger
 
#endif // TRIGGER_PLUGINS_LIVETIMECOUNTER_HPP_
