/**
 * @file ModuleLevelTrigger.cpp ModuleLevelTrigger class
 * implementation
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "ModuleLevelTrigger.hpp"

#include "daqdataformats/ComponentRequest.hpp"

#include "dfmessages/TimeSync.hpp"
#include "dfmessages/TriggerDecision.hpp"
#include "dfmessages/TriggerInhibit.hpp"
#include "dfmessages/Types.hpp"
#include "logging/Logging.hpp"

#include "trigger/Issues.hpp"
#include "trigger/moduleleveltrigger/Nljs.hpp"
#include "trigger/LivetimeCounter.hpp"

#include "timinglibs/TimestampEstimator.hpp"

#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/app/Nljs.hpp"
#include "networkmanager/NetworkManager.hpp"

#include <algorithm>
#include <cassert>
#include <pthread.h>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

namespace dunedaq {
namespace trigger {

ModuleLevelTrigger::ModuleLevelTrigger(const std::string& name)
  : DAQModule(name)
  , m_last_trigger_number(0)
  , m_run_number(0)
{
  // clang-format off
  register_command("conf",   &ModuleLevelTrigger::do_configure);
  register_command("start",  &ModuleLevelTrigger::do_start);
  register_command("stop",   &ModuleLevelTrigger::do_stop);
  register_command("pause",  &ModuleLevelTrigger::do_pause);
  register_command("resume", &ModuleLevelTrigger::do_resume);
  register_command("scrap",  &ModuleLevelTrigger::do_scrap);
  // clang-format on
}

void
ModuleLevelTrigger::init(const nlohmann::json& iniobj)
{
  m_candidate_source.reset(
    new appfwk::DAQSource<triggeralgs::TriggerCandidate>(appfwk::queue_inst(iniobj, "trigger_candidate_source")));
}

void
ModuleLevelTrigger::get_info(opmonlib::InfoCollector& ci, int /*level*/)
{
  moduleleveltriggerinfo::Info i;

  i.tc_received_count = m_tc_received_count.load();

  i.td_sent_count = m_td_sent_count.load();
  i.td_inhibited_count = m_td_inhibited_count.load();
  i.td_paused_count = m_td_paused_count.load();
  i.td_total_count = m_td_total_count.load();

  if (m_livetime_counter.get() != nullptr) {
    i.lc_kLive = m_livetime_counter->get_time(LivetimeCounter::State::kLive);
    i.lc_kPaused = m_livetime_counter->get_time(LivetimeCounter::State::kPaused);
    i.lc_kDead = m_livetime_counter->get_time(LivetimeCounter::State::kDead);
  }

  ci.add(i);
}

void
ModuleLevelTrigger::do_configure(const nlohmann::json& confobj)
{
  auto params = confobj.get<moduleleveltrigger::ConfParams>();

  // set readout times
  m_readout_window_map[params.c0.candidate_type] = { params.c0.time_before, params.c0.time_after };
  m_readout_window_map[params.c1.candidate_type] = { params.c1.time_before, params.c1.time_after };
  m_readout_window_map[params.c2.candidate_type] = { params.c2.time_before, params.c2.time_after };
  m_readout_window_map[params.c3.candidate_type] = { params.c3.time_before, params.c3.time_after };
  m_readout_window_map[params.c4.candidate_type] = { params.c4.time_before, params.c4.time_after };
  m_readout_window_map[params.c5.candidate_type] = { params.c5.time_before, params.c5.time_after };
  m_readout_window_map[params.c6.candidate_type] = { params.c6.time_before, params.c6.time_after };
  m_readout_window_map[params.c7.candidate_type] = { params.c7.time_before, params.c7.time_after };
  m_buffer_timeout = params.buffer_timeout;
  m_td_out_of_timeout = params.td_out_of_timeout;
  TLOG(3) << "TD out of timeout: " << m_td_out_of_timeout;

  m_links.clear();
  for (auto const& link : params.links) {
    m_links.push_back(
      dfmessages::GeoID{ daqdataformats::GeoID::string_to_system_type(link.system), link.region, link.element });
  }
  m_trigger_decision_connection = params.dfo_connection;
  m_inhibit_connection = params.dfo_busy_connection;
  m_hsi_passthrough = params.hsi_trigger_type_passthrough;

  networkmanager::NetworkManager::get().start_listening(m_inhibit_connection);
  m_configured_flag.store(true);
}

void
ModuleLevelTrigger::do_start(const nlohmann::json& startobj)
{
  m_run_number = startobj.value<dunedaq::daqdataformats::run_number_t>("run", 0);

  m_paused.store(true);
  m_running_flag.store(true);
  m_dfo_is_busy.store(false);

  m_livetime_counter.reset(new LivetimeCounter(LivetimeCounter::State::kPaused));

  networkmanager::NetworkManager::get().register_callback(
    m_inhibit_connection, std::bind(&ModuleLevelTrigger::dfo_busy_callback, this, std::placeholders::_1));

  m_send_trigger_decisions_thread = std::thread(&ModuleLevelTrigger::send_trigger_decisions, this);
  pthread_setname_np(m_send_trigger_decisions_thread.native_handle(), "mlt-trig-dec");
  ers::info(TriggerStartOfRun(ERS_HERE, m_run_number));
}

void
ModuleLevelTrigger::do_stop(const nlohmann::json& /*stopobj*/)
{
  m_running_flag.store(false);
  m_send_trigger_decisions_thread.join();
  
  m_lc_deadtime = m_livetime_counter->get_time(LivetimeCounter::State::kDead) + m_livetime_counter->get_time(LivetimeCounter::State::kPaused);
  TLOG(3) << "LivetimeCounter - total deadtime+paused: " << m_lc_deadtime << std::endl;
  m_livetime_counter.reset(); // Calls LivetimeCounter dtor?

  networkmanager::NetworkManager::get().clear_callback(m_inhibit_connection);
  ers::info(TriggerEndOfRun(ERS_HERE, m_run_number));
}

void
ModuleLevelTrigger::do_pause(const nlohmann::json& /*pauseobj*/)
{
  m_paused.store(true);
  m_livetime_counter->set_state(LivetimeCounter::State::kPaused);
  TLOG() << "******* Triggers PAUSED! *********";
  ers::info(TriggerPaused(ERS_HERE));
}

void
ModuleLevelTrigger::do_resume(const nlohmann::json& /*resumeobj*/)
{
  ers::info(TriggerActive(ERS_HERE));
  TLOG() << "******* Triggers RESUMED! *********";
  m_livetime_counter->set_state(LivetimeCounter::State::kLive);
  m_paused.store(false);
}

void
ModuleLevelTrigger::do_scrap(const nlohmann::json& /*scrapobj*/)
{
  m_links.clear();
  networkmanager::NetworkManager::get().stop_listening(m_inhibit_connection);
  m_configured_flag.store(false);
}

dfmessages::TriggerDecision
ModuleLevelTrigger::create_decision()
{
  dfmessages::TriggerDecision decision;
  decision.trigger_number = m_last_trigger_number + 1;
  decision.run_number = m_run_number;
  decision.trigger_timestamp = m_tc_loop.back().time_candidate;
  decision.readout_type = dfmessages::ReadoutType::kLocalized;

  if (m_hsi_passthrough == true){
    if (m_tc_loop.back().type == triggeralgs::TriggerCandidate::Type::kTiming){
      decision.trigger_type = m_tc_loop.back().detid & 0xff;
    } else {
      m_trigger_type_shifted = ((int)m_tc_loop.back().type << 8);
      decision.trigger_type = m_trigger_type_shifted;
    }
  } else {
    decision.trigger_type = 1; // m_trigger_type;
  }


  for (auto link : m_links) {
    dfmessages::ComponentRequest request;
    request.component = link;
    request.window_begin = m_readout_start;
    request.window_end = m_readout_end;

    decision.components.push_back(request);
  }

  clean_tc_queue();
  return decision;
}

void
ModuleLevelTrigger::send_trigger_decisions()
{

  // We get here at start of run, so reset the trigger number
  m_last_trigger_number = 0;

  // OpMon.
  m_tc_received_count.store(0);
  m_td_sent_count.store(0);
  m_td_inhibited_count.store(0);
  m_td_paused_count.store(0);
  m_td_total_count.store(0);
  m_lc_kLive.store(0);
  m_lc_kPaused.store(0);
  m_lc_kDead.store(0);

  while (true) {
    triggeralgs::TriggerCandidate tc;
    try {
      m_candidate_source->pop(tc, std::chrono::milliseconds(100));
      ++m_tc_received_count;
    } catch (appfwk::QueueTimeoutExpired&) {
      // The condition to exit the loop is that we've been stopped and
      // there's nothing left on the input queue
      if (!m_running_flag.load()) {
        break;
      } else {
        continue;
      }
    }

    process_tc(tc);
    TLOG(3) << "TC loop size: " << m_tc_loop.size();
    if (!create_decision_check()){ continue; }

    if (!m_paused.load() && !m_dfo_is_busy.load() ) {

      dfmessages::TriggerDecision decision = create_decision();

      TLOG_DEBUG(1) << "Sending a decision with triggernumber " << decision.trigger_number << " timestamp "
                    << decision.trigger_timestamp << " number of links " << decision.components.size()
                    << " based on TC of type " << static_cast<std::underlying_type_t<decltype(tc.type)>>(tc.type);

      try {
        auto serialised_decision = serialization::serialize(decision, serialization::kMsgPack);
        networkmanager::NetworkManager::get().send_to(m_trigger_decision_connection,
                                                      static_cast<const void*>(serialised_decision.data()),
                                                      serialised_decision.size(),
                                                      std::chrono::milliseconds(1));
        m_td_sent_count++;
        m_last_trigger_number++;
      } catch (const ers::Issue& e) {
	ers::error(e);
        TLOG_DEBUG(1) << "The network is misbehaving: it accepted TD but the send failed for "
                      << tc.time_candidate;
        m_td_queue_timeout_expired_err_count++;
      }

    } else if (m_paused.load()) {
      ++m_td_paused_count;
      TLOG_DEBUG(1) << "Triggers are paused. Not sending a TriggerDecision ";
    } else {
      ers::warning(TriggerInhibited(ERS_HERE, m_run_number));
      TLOG_DEBUG(1) << "The DFO is busy. Not sending a TriggerDecision for candidate timestamp "
                    << tc.time_candidate;
      m_td_inhibited_count++;
    }
    m_td_total_count++;
  }

  TLOG() << "Run " << m_run_number << ": "
         << "Received " << m_tc_received_count << " TCs. Sent " << m_td_sent_count.load() << " TDs. "
         << m_td_paused_count << " TDs were created during pause, and " << m_td_inhibited_count.load()
         << " TDs were inhibited.";

  m_lc_kLive_count = m_livetime_counter->get_time(LivetimeCounter::State::kLive);
  m_lc_kPaused_count = m_livetime_counter->get_time(LivetimeCounter::State::kPaused);
  m_lc_kDead_count = m_livetime_counter->get_time(LivetimeCounter::State::kDead);
  m_lc_kLive = m_lc_kLive_count;
  m_lc_kPaused = m_lc_kPaused_count;
  m_lc_kDead = m_lc_kDead_count; 

  m_lc_deadtime = m_livetime_counter->get_time(LivetimeCounter::State::kDead) + m_livetime_counter->get_time(LivetimeCounter::State::kPaused);
}

void
ModuleLevelTrigger::dfo_busy_callback(ipm::Receiver::Response message)
{

  auto inhibit = serialization::deserialize<dfmessages::TriggerInhibit>(message.data);

  if (inhibit.run_number == m_run_number) {
    m_dfo_is_busy = inhibit.busy;
    m_livetime_counter->set_state(LivetimeCounter::State::kDead);
  }
}

void
ModuleLevelTrigger::process_tc(const triggeralgs::TriggerCandidate& tc)
{
  m_tc_loop.push_back(tc);
  if (m_tc_loop.size() > 1) { 
    update_readout_window(tc); 
  } else { 
    check_overlap(tc);
    try {
      if (m_td_out_of_timeout_flag == true){
        throw dunedaq::trigger::TCOutOfTimeout(ERS_HERE, get_name(), tc.time_candidate);
      }
    } catch (TCOutOfTimeout& e) {
      ers::error(e);
      return;
    }
    if ((m_td_out_of_timeout == true) & (m_td_out_of_timeout_flag == true)) {
      // in this case completely drop the tc
      m_tc_loop.clear();
      return;
    }
    set_readout_window(tc); 
  }
  set_endtime();
}

void
ModuleLevelTrigger::set_readout_window(const triggeralgs::TriggerCandidate& tc)
{
  m_readout_before = m_readout_window_map[(int)tc.type].first;
  m_readout_after = m_readout_window_map[(int)tc.type].second;
  m_readout_start = tc.time_candidate - m_readout_before;
  m_readout_end = tc.time_candidate + m_readout_after;
  TLOG(3) << "Set new readout window as: " << m_readout_start << " " << m_readout_end; 
}

void
ModuleLevelTrigger::update_readout_window(const triggeralgs::TriggerCandidate& tc)
{ 
  if ((tc.time_candidate - m_readout_window_map[(int)tc.type].first) < m_readout_start) {
    m_readout_start = tc.time_candidate - m_readout_window_map[(int)tc.type].first;
    TLOG(3) << "Updated readout window start: " << m_readout_start;
  }
  if ((tc.time_candidate + m_readout_window_map[(int)tc.type].second) > m_readout_end) {
    m_readout_end = tc.time_candidate + m_readout_window_map[(int)tc.type].second;
    TLOG(3) << "Updated readout window end: " << m_readout_end;
  }
}

void
ModuleLevelTrigger::set_endtime()
{
  m_endtime = m_readout_end + m_buffer_timeout;
  TLOG(3) << "New endtime set as: " << m_endtime;
}

bool
ModuleLevelTrigger::create_decision_check()
{
  auto m_timestamp_endtime = m_endtime/50; // to get ms from 50 MHz ticks
  int64_t m_timestamp_now = std::chrono::duration_cast<std::chrono::microseconds>(system_clock::now().time_since_epoch()).count();
  int64_t m_timestamp_diff = m_timestamp_now - m_timestamp_endtime;
  TLOG(3) << "Time diff now - endtime (ms): " << m_timestamp_diff;
  if (m_timestamp_diff >= 0) {
    return true;
  } else {
    return false;
  }
}

void
ModuleLevelTrigger::clean_tc_queue()
{
  m_tc_loop.clear();
  m_previous_endtime = m_endtime;
  m_td_out_of_timeout_flag = false;
}

void
ModuleLevelTrigger::check_overlap(const triggeralgs::TriggerCandidate& tc)
{
  if ((tc.time_candidate - m_readout_window_map[(int)tc.type].first) < m_previous_endtime){
    m_td_out_of_timeout_flag = true;
  } else {
    m_td_out_of_timeout_flag = false;
  } 
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::ModuleLevelTrigger)

// Local Variables:
// c-basic-offset: 2
// End:
