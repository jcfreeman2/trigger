/**
 * @file FakeTPCreatorHeartbeatMaker.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2021.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "FakeTPCreatorHeartbeatMaker.hpp"

#include "appfwk/DAQModuleHelper.hpp"
#include "iomanager/IOManager.hpp"
#include "rcif/cmd/Nljs.hpp"
#include "trigger/Issues.hpp"

#include <chrono>
#include <daqdataformats/Types.hpp>
#include <stdexcept>
#include <string>
#include <thread>

namespace dunedaq {
namespace trigger {
FakeTPCreatorHeartbeatMaker::FakeTPCreatorHeartbeatMaker(const std::string& name)
  : DAQModule(name)
  , m_thread(std::bind(&FakeTPCreatorHeartbeatMaker::do_work, this, std::placeholders::_1))
  , m_input_queue(nullptr)
  , m_output_queue(nullptr)
  , m_queue_timeout(100)
  , m_last_seen_timestamp(0)
{

  register_command("conf", &FakeTPCreatorHeartbeatMaker::do_conf);
  register_command("start", &FakeTPCreatorHeartbeatMaker::do_start);
  register_command("stop", &FakeTPCreatorHeartbeatMaker::do_stop);
  register_command("scrap", &FakeTPCreatorHeartbeatMaker::do_scrap);
}

void
FakeTPCreatorHeartbeatMaker::init(const nlohmann::json& iniobj)
{
  try {
    m_input_queue = get_iom_receiver<trigger::TPSet>(appfwk::connection_inst(iniobj, "tpset_source"));
    m_output_queue = get_iom_sender<trigger::TPSet>(appfwk::connection_inst(iniobj, "tpset_sink"));
  } catch (const ers::Issue& excpt) {
    throw dunedaq::trigger::InvalidQueueFatalError(ERS_HERE, get_name(), "input/output", excpt);
  }
}

void
FakeTPCreatorHeartbeatMaker::get_info(opmonlib::InfoCollector& ci, int /*level*/)
{
  faketpcreatorheartbeatmakerinfo::Info i;

  i.tpset_received_count = m_tpset_received_count.load();
  i.tpset_sent_count = m_tpset_sent_count.load();
  i.heartbeats_sent = m_heartbeats_sent.load();

  ci.add(i);
}

void
FakeTPCreatorHeartbeatMaker::do_conf(const nlohmann::json& conf)
{
  m_conf = conf.get<dunedaq::trigger::faketpcreatorheartbeatmaker::Conf>();
  TLOG_DEBUG(2) << get_name() + " configured.";
}

void
FakeTPCreatorHeartbeatMaker::do_start(const nlohmann::json& args)
{
  rcif::cmd::StartParams start_params = args.get<rcif::cmd::StartParams>();
  m_run_number = start_params.run;

  m_thread.start_working_thread("heartbeater");
  TLOG_DEBUG(2) << get_name() + " successfully started.";
}

void
FakeTPCreatorHeartbeatMaker::do_stop(const nlohmann::json&)
{
  m_thread.stop_working_thread();
  TLOG_DEBUG(2) << get_name() + " successfully stopped.";
}

void
FakeTPCreatorHeartbeatMaker::do_scrap(const nlohmann::json&)
{}

void
FakeTPCreatorHeartbeatMaker::do_work(std::atomic<bool>& running_flag)
{
  // OpMon.
  m_tpset_received_count.store(0);
  m_tpset_sent_count.store(0);
  m_heartbeats_sent.store(0);

  daqdataformats::timestamp_t last_sent_set_time = 0;

  TPSet::seqno_t sequence_number = 0;

  auto start_time = std::chrono::steady_clock::now();
  
  while (true) {
    TPSet payload_tpset;
    bool got_payload = false;
    try {
      payload_tpset = m_input_queue->receive(std::chrono::milliseconds(0));
      m_tpset_received_count++;
      got_payload = true;

      if (payload_tpset.start_time < last_sent_set_time) {
        ers::warning(trigger::EarlyPayloadTPSet(ERS_HERE, get_name(), last_sent_set_time, payload_tpset.start_time));
      }
      
      m_last_seen_timestamp = payload_tpset.start_time;
      m_last_seen_wall_clock = std::chrono::steady_clock::now();
      if (m_geoid.region_id == daqdataformats::GeoID::s_invalid_region_id) {
        m_geoid = payload_tpset.origin;
      }
    } catch (const dunedaq::iomanager::TimeoutExpired& excpt) {
      // The condition to exit the loop is that we've been stopped and
      // there's nothing left on the input queue
      if (!running_flag.load()) {
        break;
      }
    }

    daqdataformats::timestamp_t timestamp_now = get_timestamp_lower_bound();
    if (timestamp_now == 0) {
      // We haven't received any inputs yet, so we don't send any heartbeats
      continue;
    }
    daqdataformats::timestamp_t offset_ticks = m_conf.clock_frequency_hz*m_conf.heartbeat_send_offset_ms/1000;
    daqdataformats::timestamp_t a = timestamp_now > offset_ticks ? (timestamp_now - offset_ticks) : 0;
    daqdataformats::timestamp_t timestamp_for_heartbeats = std::max(a, m_last_seen_timestamp);
    std::vector<TPSet> output_sets;
    // If last_sent_set_time is zero, don't try to get heartbeats,
    // otherwise we try to generate all of the heartbeats since the
    // start of time
    if (last_sent_set_time != 0) {
      output_sets = get_heartbeat_sets(last_sent_set_time, timestamp_for_heartbeats, m_conf.heartbeat_interval);
    }

    // The payload always goes _after_ the heartbeats, which have start_times <= to it
    if (got_payload) {
      output_sets.push_back(payload_tpset);
    }
    
    for(auto& output_set : output_sets) {
      output_set.seqno = sequence_number;
      ++sequence_number;
      if (output_set.start_time < last_sent_set_time) {
        throw std::logic_error("foo");
      }
      last_sent_set_time = output_set.start_time;
      try {
        bool is_payload = (output_set.type == TPSet::kPayload);
        m_output_queue->send(std::move(output_set), m_queue_timeout);
        if (is_payload) {
          ++m_tpset_sent_count;
        }
        else {
          ++m_heartbeats_sent;
        }
      } catch (const dunedaq::iomanager::TimeoutExpired& excpt) {
        std::ostringstream oss_warn;
        oss_warn << "push to output queue \"" << m_output_queue->get_name() << "\"";
        ers::warning(
                     dunedaq::iomanager::TimeoutExpired(ERS_HERE, get_name(), oss_warn.str(), m_queue_timeout.count()));
      }
    } // end loop over heartbeats
    
    if (!got_payload) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
  } // while (true)

  auto end_time = std::chrono::steady_clock::now();
  auto run_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  TLOG() << get_name() << ": Ran for " << run_ms << "ms. Received " << m_tpset_received_count << " and sent " << m_tpset_sent_count << " real TPSets. Sent "
         << m_heartbeats_sent << " fake heartbeats.";
}

daqdataformats::timestamp_t
FakeTPCreatorHeartbeatMaker::get_timestamp_lower_bound() const
{
  if (m_last_seen_timestamp == 0) return 0;

  using namespace std::chrono;

  auto us_since_last_seen = duration_cast<microseconds>(steady_clock::now() - m_last_seen_wall_clock).count();
  return m_last_seen_timestamp + (m_conf.clock_frequency_hz * us_since_last_seen / 1000000);
}

std::vector<TPSet>
FakeTPCreatorHeartbeatMaker::get_heartbeat_sets(daqdataformats::timestamp_t last_sent_timestamp,
                                                daqdataformats::timestamp_t timestamp_now,
                                                dunedaq::trigger::faketpcreatorheartbeatmaker::ticks heartbeat_interval) const
{
  TLOG_DEBUG(3) << get_name() << ": get_heartbeat_sets with last_sent_timestamp = " << last_sent_timestamp << ", timestamp_now = " << timestamp_now << ", heartbeat_interval = " << heartbeat_interval;
  std::vector<TPSet> ret;
  // Round up last_sent_timestamp to the next multiple of heartbeat_interval
  daqdataformats::timestamp_t next_heartbeat = (last_sent_timestamp/heartbeat_interval + 1) * heartbeat_interval;
  
  while (next_heartbeat <= timestamp_now) {
    TPSet tpset;
    tpset.type = TPSet::Type::kHeartbeat;
    tpset.start_time = next_heartbeat;
    tpset.end_time = next_heartbeat;
    tpset.run_number = m_run_number;
    tpset.origin = m_geoid;

    ret.push_back(tpset);

    next_heartbeat += heartbeat_interval;
  }

  return ret;
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::FakeTPCreatorHeartbeatMaker)
