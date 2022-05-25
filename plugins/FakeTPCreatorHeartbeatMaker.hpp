/**
 * @file FakeTPCreatorHeartbeatMaker.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2021.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_PLUGINS_FAKETPCREATORHEARTBEATMAKER_HPP_
#define TRIGGER_PLUGINS_FAKETPCREATORHEARTBEATMAKER_HPP_

#include "trigger/Issues.hpp"
#include "trigger/TPSet.hpp"
#include "trigger/faketpcreatorheartbeatmaker/Nljs.hpp"
#include "trigger/faketpcreatorheartbeatmakerinfo/InfoNljs.hpp"

#include "appfwk/DAQModule.hpp"
#include "iomanager/Receiver.hpp"
#include "iomanager/Sender.hpp"
#include "utilities/WorkerThread.hpp"

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace dunedaq {
namespace trigger {
class FakeTPCreatorHeartbeatMaker : public dunedaq::appfwk::DAQModule
{
public:
  explicit FakeTPCreatorHeartbeatMaker(const std::string& name);

  FakeTPCreatorHeartbeatMaker(const FakeTPCreatorHeartbeatMaker&) = delete;
  FakeTPCreatorHeartbeatMaker& operator=(const FakeTPCreatorHeartbeatMaker&) = delete;
  FakeTPCreatorHeartbeatMaker(FakeTPCreatorHeartbeatMaker&&) = delete;
  FakeTPCreatorHeartbeatMaker& operator=(FakeTPCreatorHeartbeatMaker&&) = delete;

  void init(const nlohmann::json& iniobj) override;
  void get_info(opmonlib::InfoCollector& ci, int level) override;

private:
  void do_conf(const nlohmann::json& config);
  void do_start(const nlohmann::json& obj);
  void do_stop(const nlohmann::json& obj);
  void do_scrap(const nlohmann::json& obj);
  void do_work(std::atomic<bool>&);

  daqdataformats::timestamp_t get_timestamp_lower_bound() const;

  // Get all the TPSets that should be sent for timestamps between
  // `last_sent_timestamp` and `timestamp_now`, with the given
  // `heartbeat_interval`
  std::vector<TPSet> get_heartbeat_sets(daqdataformats::timestamp_t last_sent_timestamp,
                                        daqdataformats::timestamp_t timestamp_now,
                                        dunedaq::trigger::faketpcreatorheartbeatmaker::ticks heartbeat_interval) const;
  
  dunedaq::utilities::WorkerThread m_thread;

  using source_t = dunedaq::iomanager::ReceiverConcept<TPSet>;
  std::shared_ptr<source_t> m_input_queue;
  using sink_t = dunedaq::iomanager::SenderConcept<TPSet>;
  std::shared_ptr<sink_t> m_output_queue;

  std::chrono::milliseconds m_queue_timeout;
  daqdataformats::timestamp_t m_last_seen_timestamp;
  std::chrono::steady_clock::time_point m_last_seen_wall_clock;
  
  dunedaq::trigger::faketpcreatorheartbeatmaker::Conf m_conf;
  
  daqdataformats::run_number_t m_run_number{ daqdataformats::TypeDefaults::s_invalid_run_number };

  daqdataformats::GeoID m_geoid{
    daqdataformats::GeoID::SystemType::kDataSelection,
    daqdataformats::GeoID::s_invalid_region_id,
    daqdataformats::GeoID::s_invalid_element_id };
  // Opmon variables
  using metric_counter_type = decltype(faketpcreatorheartbeatmakerinfo::Info::tpset_received_count);
  std::atomic<metric_counter_type> m_tpset_received_count{ 0 };
  std::atomic<metric_counter_type> m_tpset_sent_count{ 0 };
  std::atomic<metric_counter_type> m_heartbeats_sent{ 0 };
};
} // namespace trigger
} // namespace dunedaq

#endif // TRIGGER_PLUGINS_FAKETPCREATORHEARTBEATMAKER_HPP_
