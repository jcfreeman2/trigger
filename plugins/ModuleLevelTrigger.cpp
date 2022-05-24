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
  TLOG(3) << "buffer timeout: " << m_buffer_timeout;
  TLOG(3) << "TD out of timeout: " << m_td_out_of_timeout;
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
  // flush all pending TDs at run stop
  for ( PendingTD m_ready_td : m_pending_tds) {
    call_tc_decision(m_ready_td, true);
  }

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
ModuleLevelTrigger::create_decision(const ModuleLevelTrigger::PendingTD& m_pending_td)
{
  dfmessages::TriggerDecision decision;
  decision.trigger_number = m_last_trigger_number + 1;
  decision.run_number = m_run_number;
  decision.trigger_timestamp = m_pending_td.m_contributing_tcs[0].time_candidate;
  decision.readout_type = dfmessages::ReadoutType::kLocalized;

  if (m_hsi_passthrough == true){
    if (m_pending_td.m_contributing_tcs[0].type == triggeralgs::TriggerCandidate::Type::kTiming){
      decision.trigger_type = m_pending_td.m_contributing_tcs[0].detid & 0xff;
    } else {
      m_trigger_type_shifted = ((int)m_pending_td.m_contributing_tcs[0].type << 8);
      decision.trigger_type = m_trigger_type_shifted;
    }
  } else {
    decision.trigger_type = 1; // m_trigger_type;
  }

  TLOG_DEBUG(3) << "HSI passthrough: " << m_hsi_passthrough << ", TC detid: " << m_pending_td.m_contributing_tcs[0].detid << ", TC type: " << (int)m_pending_td.m_contributing_tcs[0].type << ", DECISION trigger type: " << decision.trigger_type;

  for (auto link : m_links) {
    dfmessages::ComponentRequest request;
    request.component = link;
    request.window_begin = m_pending_td.m_readout_start;
    request.window_end = m_pending_td.m_readout_end;

    decision.components.push_back(request);
  }

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

  // New buffering logic here
  while (m_running_flag) {
    triggeralgs::TriggerCandidate tc;
    try {
      m_candidate_source->pop(tc, std::chrono::milliseconds(100));
      ++m_tc_received_count;
      add_tc(tc);
      TLOG(3) << "pending tds size: " << m_pending_tds.size();
    } catch (appfwk::QueueTimeoutExpired&) {
      // The condition to exit the loop is that we've been stopped and
      // there's nothing left on the input queue
      if (!m_running_flag.load()) {
        break;
      }
    }

    m_ready_tds = get_ready_tds(m_pending_tds);
    TLOG(3) << "ready tds: " << m_ready_tds.size();
    TLOG(3) << "updated pending tds: " << m_pending_tds.size();
    TLOG(3) << "sent tds: " << m_sent_tds.size();

    if (m_ready_tds.size() > 0) {
      for (int i=0; i<m_ready_tds.size(); i++) { 
        try { 
          if (check_overlap_td( m_ready_tds[i] )) {
            throw dunedaq::trigger::TCOutOfTimeout(ERS_HERE, get_name(), m_ready_tds[i].m_contributing_tcs[0].time_candidate); 
          }
        } catch (TCOutOfTimeout& e) {
          ers::error(e);
        }
        if ((check_overlap_td(m_ready_tds[i])) & (!m_td_out_of_timeout)) { // if this is not set, drop the td
          m_ready_tds.erase( m_ready_tds.begin() + i );
          TLOG(3) << "overlapping previous TD, dropping!";
          continue;
        }
        call_tc_decision(m_ready_tds[i]);
        add_td(m_ready_tds[i]);
      }
    } 

    TLOG(3) << "updated sent tds: " << m_sent_tds.size();

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
ModuleLevelTrigger::call_tc_decision(const ModuleLevelTrigger::PendingTD& m_pending_td, bool override_flag) {
  TLOG(3) << "Override?: " << override_flag;
  if ( (!m_paused.load() && !m_dfo_is_busy.load()) | override_flag ) {

    dfmessages::TriggerDecision decision = create_decision(m_pending_td);

    TLOG_DEBUG(1) << "Sending a decision with triggernumber " << decision.trigger_number << " timestamp "
                  << decision.trigger_timestamp << " number of links " << decision.components.size()
                  << " based on TC of type " << static_cast<std::underlying_type_t<decltype(m_pending_td.m_contributing_tcs[0].type)>>(m_pending_td.m_contributing_tcs[0].type);

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
                    << m_pending_td.m_contributing_tcs[0].time_candidate;
      m_td_queue_timeout_expired_err_count++;
    }

  } else if (m_paused.load()) {
    ++m_td_paused_count;
    TLOG_DEBUG(1) << "Triggers are paused. Not sending a TriggerDecision ";
  } else {
    ers::warning(TriggerInhibited(ERS_HERE, m_run_number));
    TLOG_DEBUG(1) << "The DFO is busy. Not sending a TriggerDecision for candidate timestamp "
                  << m_pending_td.m_contributing_tcs[0].time_candidate;
    m_td_inhibited_count++;
  }
  m_td_total_count++;
}

void
ModuleLevelTrigger::add_tc(const triggeralgs::TriggerCandidate& tc) {
  bool m_added_to_existing = false;

  if (m_pending_tds.size() > 0) {
    for (int m_pending_td; m_pending_td<m_pending_tds.size(); m_pending_td++) {
       PendingTD m_temp_pending_td = m_pending_tds[m_pending_td];
       if (check_overlap(tc, m_temp_pending_td)) {
         TLOG(3) << "These overlap!";
         m_temp_pending_td.m_contributing_tcs.push_back(tc);
         m_temp_pending_td.m_readout_start = ( (tc.time_candidate - m_readout_window_map[(int)tc.type].first) >= m_temp_pending_td.m_readout_start) ? m_temp_pending_td.m_readout_start : (tc.time_candidate - m_readout_window_map[(int)tc.type].first);
         m_temp_pending_td.m_readout_end = ( (tc.time_candidate + m_readout_window_map[(int)tc.type].second) >= m_temp_pending_td.m_readout_end) ? (tc.time_candidate + m_readout_window_map[(int)tc.type].second) : m_temp_pending_td.m_readout_end;
         m_temp_pending_td.m_walltime_expiration = m_temp_pending_td.m_readout_end + m_buffer_timeout;
         m_added_to_existing = true;
         break;
       }
    }
  }
  
  if (!m_added_to_existing) {
    PendingTD td_candidate;
    td_candidate.m_contributing_tcs.push_back(tc);
    td_candidate.m_readout_start = tc.time_candidate - m_readout_window_map[(int)tc.type].first;
    td_candidate.m_readout_end = tc.time_candidate + m_readout_window_map[(int)tc.type].second;
    td_candidate.m_walltime_expiration = td_candidate.m_readout_end/50000 + m_buffer_timeout;
    m_pending_tds.push_back(td_candidate);
  }
}

bool
ModuleLevelTrigger::check_overlap(const triggeralgs::TriggerCandidate& tc, const PendingTD& m_pending_td) {
  bool m_overlap = false;
  
  if ( ( (tc.time_candidate - m_readout_window_map[(int)tc.type].first) >= m_pending_td.m_readout_start) & ( (tc.time_candidate - m_readout_window_map[(int)tc.type].first) <= m_pending_td.m_readout_end)
       | ( (tc.time_candidate + m_readout_window_map[(int)tc.type].second) >= m_pending_td.m_readout_start) & ( (tc.time_candidate + m_readout_window_map[(int)tc.type].second) <= m_pending_td.m_readout_end) ) {
    m_overlap = true;
  } 
  return m_overlap;
}

bool
ModuleLevelTrigger::check_overlap_td(const PendingTD& m_pending_td) {
  bool m_overlap = false;

  for (PendingTD m_sent_td : m_sent_tds) {
    if ( ((m_pending_td.m_readout_start >= m_sent_td.m_readout_start) & (m_pending_td.m_readout_start <= m_sent_td.m_readout_end))
       | ((m_pending_td.m_readout_end >= m_sent_td.m_readout_start) & (m_pending_td.m_readout_end <= m_sent_td.m_readout_end)) ) {
      m_overlap = true;
      break;
    }
  }
  return m_overlap;
}

void
ModuleLevelTrigger::add_td(const PendingTD& m_pending_td) {
  m_sent_tds.push_back(m_pending_td);
  while (m_sent_tds.size() > 20) {
    m_sent_tds.erase( m_sent_tds.begin() );
  }
}

std::vector <ModuleLevelTrigger::PendingTD>
ModuleLevelTrigger::get_ready_tds(std::vector <PendingTD>& m_pending_tds) {
  std::vector <PendingTD> m_return_tds;
  for (int m_pending_td; m_pending_td<m_pending_tds.size(); m_pending_td++) {
    m_timestamp_now = std::chrono::duration_cast<std::chrono::milliseconds>(system_clock::now().time_since_epoch()).count(); 
    if ( m_timestamp_now >= m_pending_tds[m_pending_td].m_walltime_expiration ) {
      m_return_tds.push_back(m_pending_tds[m_pending_td]);
      m_pending_tds.erase(m_pending_tds.begin()+m_pending_td);
    }
  }
  return m_return_tds; 
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

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::ModuleLevelTrigger)

// Local Variables:
// c-basic-offset: 2
// End:
