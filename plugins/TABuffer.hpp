/**
 * @file TABuffer.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2021.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_PLUGINS_TABUFFER_HPP_
#define TRIGGER_PLUGINS_TABUFFER_HPP_

#include "appfwk/DAQModule.hpp"
#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/DAQSink.hpp"
#include "appfwk/DAQSource.hpp"
#include "daqdataformats/Fragment.hpp"
#include "readoutlibs/FrameErrorRegistry.hpp"
#include "readoutlibs/models/DefaultRequestHandlerModel.hpp"
#include "readoutlibs/models/BinarySearchQueueModel.hpp"
#include "triggeralgs/TriggerPrimitive.hpp"
#include "utilities/WorkerThread.hpp"

#include "trigger/Issues.hpp"
#include "trigger/TASet.hpp"

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <utility>

namespace dunedaq {
namespace trigger {
class TABuffer : public dunedaq::appfwk::DAQModule
{
public:
  explicit TABuffer(const std::string& name);

  TABuffer(const TABuffer&) = delete;
  TABuffer& operator=(const TABuffer&) = delete;
  TABuffer(TABuffer&&) = delete;
  TABuffer& operator=(TABuffer&&) = delete;

  void init(const nlohmann::json& iniobj) override;
  void get_info(opmonlib::InfoCollector& ci, int level) override;

private:

  struct TAWrapper
  {
    triggeralgs::TriggerActivity activity;

    // Don't really want this default ctor, but IterableQueueModel requires it
    TAWrapper() {}
    
    TAWrapper(triggeralgs::TriggerActivity p)
      : activity(p)
    {}
    
    // comparable based on first timestamp
    bool operator<(const TAWrapper& other) const
    {
      return this->activity.time_start < other.activity.time_start;
    }

    uint64_t get_first_timestamp() const // NOLINT(build/unsigned)
    {
      return activity.time_start;
    }

    void set_first_timestamp(uint64_t ts) // NOLINT(build/unsigned)
    {
      activity.time_start = ts;
    }

    uint64_t get_timestamp() const // NOLINT(build/unsigned)
    {
      return activity.time_start;
    }


    size_t get_payload_size() { return sizeof(triggeralgs::TriggerActivity); }

    size_t get_num_frames() { return 1; }

    size_t get_frame_size() { return get_payload_size(); }

    triggeralgs::TriggerActivity* begin()
    {
      return &activity;
    }
    
    triggeralgs::TriggerActivity* end()
    {
      return &activity + 1;
    }

    //static const constexpr size_t fixed_payload_size = 5568;
    static const constexpr daqdataformats::GeoID::SystemType system_type = daqdataformats::GeoID::SystemType::kDataSelection;
    static const constexpr daqdataformats::FragmentType fragment_type = daqdataformats::FragmentType::kTriggerActivities;
    // No idea what this should really be set to
    static const constexpr uint64_t expected_tick_difference = 16; // NOLINT(build/unsigned)
};

  void do_conf(const nlohmann::json& config);
  void do_start(const nlohmann::json& obj);
  void do_stop(const nlohmann::json& obj);
  void do_scrap(const nlohmann::json& obj);
  void do_work(std::atomic<bool>&);

  dunedaq::utilities::WorkerThread m_thread;

  using tas_source_t = dunedaq::appfwk::DAQSource<trigger::TASet>;
  std::unique_ptr<tas_source_t> m_input_queue_tas{nullptr};

  using dr_source_t = dunedaq::appfwk::DAQSource<dfmessages::DataRequest>;
  std::unique_ptr<dr_source_t> m_input_queue_dr{nullptr};

  using fragment_sink_t = dunedaq::appfwk::DAQSink<std::pair<std::unique_ptr<daqdataformats::Fragment>, std::string>>;
  std::unique_ptr<fragment_sink_t> m_output_queue_frag{nullptr};

  std::chrono::milliseconds m_queue_timeout;

  using buffer_object_t = TAWrapper;
  using latency_buffer_t = readoutlibs::BinarySearchQueueModel<buffer_object_t>;
  std::unique_ptr<latency_buffer_t> m_latency_buffer_impl{nullptr};
  using request_handler_t = readoutlibs::DefaultRequestHandlerModel<buffer_object_t, latency_buffer_t>;
  std::unique_ptr<request_handler_t> m_request_handler_impl{nullptr};

  // Don't actually use this, but it's currently needed as arg to request handler ctor
  std::unique_ptr<readoutlibs::FrameErrorRegistry> m_error_registry;
};
} // namespace trigger
} // namespace dunedaq

namespace dunedaq {
namespace readoutlibs {

template<>
uint64_t
get_frame_iterator_timestamp(triggeralgs::TriggerActivity* activity)
{
  return activity->time_start;
}

}
}

#endif // TRIGGER_PLUGINS_TABUFFER_HPP_
