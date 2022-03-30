/**
 * @file IntervalTCCreator.cpp IntervalTCCreator class
 * implementation
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "IntervalTCCreator.hpp"

#include "daqdataformats/ComponentRequest.hpp"

#include "dfmessages/TimeSync.hpp"
#include "dfmessages/TriggerDecision.hpp"
#include "dfmessages/TriggerInhibit.hpp"
#include "dfmessages/Types.hpp"
#include "logging/Logging.hpp"
#include "triggeralgs/TriggerCandidate.hpp"

#include "timinglibs/TimestampEstimator.hpp"
#include "timinglibs/TimestampEstimatorSystem.hpp"
#include "trigger/Issues.hpp"

#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/app/Nljs.hpp"
#include "detdataformats/trigger/Types.hpp"

#include <algorithm>
#include <cassert>
#include <pthread.h>
#include <random>
#include <string>
#include <vector>

namespace dunedaq {
namespace trigger {

IntervalTCCreator::IntervalTCCreator(const std::string& name)
  : DAQModule(name)
  , m_time_sync_source(nullptr)
  , m_trigger_candidate_sink(nullptr)
  , m_last_trigger_number(0)
  , m_run_number(0)
{
  register_command("conf", &IntervalTCCreator::do_configure);
  register_command("start", &IntervalTCCreator::do_start);
  register_command("stop", &IntervalTCCreator::do_stop);
  register_command("scrap", &IntervalTCCreator::do_scrap);
}

void
IntervalTCCreator::init(const nlohmann::json& iniobj)
{
  m_time_sync_source.reset(new appfwk::DAQSource<dfmessages::TimeSync>(appfwk::queue_inst(iniobj, "time_sync_source")));
  m_trigger_candidate_sink.reset(
    new appfwk::DAQSink<triggeralgs::TriggerCandidate>(appfwk::queue_inst(iniobj, "trigger_candidate_sink")));
}

void
IntervalTCCreator::get_info(opmonlib::InfoCollector& /*ci*/, int /*level*/)
{}

void
IntervalTCCreator::do_configure(const nlohmann::json& confobj)
{
  m_conf = confobj.get<intervaltccreator::ConfParams>();
}

void
IntervalTCCreator::do_start(const nlohmann::json& startobj)
{
  m_run_number = startobj.value<dunedaq::daqdataformats::run_number_t>("run", 0);

  m_running_flag.store(true);

  switch (m_conf.timestamp_method) {
    case dunedaq::trigger::intervaltccreator::timestamp_estimation::kTimeSync:
      TLOG_DEBUG(0) << "Creating TimestampEstimator";
      m_timestamp_estimator.reset(new timinglibs::TimestampEstimator(m_time_sync_source, m_conf.clock_frequency_hz));
      break;
    case dunedaq::trigger::intervaltccreator::timestamp_estimation::kSystemClock:
      TLOG_DEBUG(0) << "Creating TimestampEstimatorSystem";
      m_timestamp_estimator.reset(new timinglibs::TimestampEstimatorSystem(m_conf.clock_frequency_hz));
      break;
  }

  m_send_trigger_candidates_thread = std::thread(&IntervalTCCreator::send_trigger_candidates, this);
  pthread_setname_np(m_send_trigger_candidates_thread.native_handle(), "tde-trig-dec");
}

void
IntervalTCCreator::do_stop(const nlohmann::json& /*stopobj*/)
{
  m_running_flag.store(false);

  m_send_trigger_candidates_thread.join();

  m_timestamp_estimator.reset(nullptr); // Calls TimestampEstimator dtor
}

void
IntervalTCCreator::do_scrap(const nlohmann::json& /*stopobj*/)
{
  m_configured_flag.store(false);
}

triggeralgs::TriggerCandidate
IntervalTCCreator::create_candidate(dfmessages::timestamp_t timestamp)
{
  static std::default_random_engine random_engine(m_run_number);

  triggeralgs::TriggerCandidate candidate;
  candidate.time_start = timestamp;
  candidate.time_end = timestamp;
  candidate.time_candidate = timestamp;
  candidate.detid = { 1 };
  candidate.type = triggeralgs::TriggerCandidate::Type::kTiming;
  candidate.algorithm = triggeralgs::TriggerCandidate::Algorithm::kHSIEventToTriggerCandidate;


  return candidate;
}

void
IntervalTCCreator::send_trigger_candidates()
{

  // We get here at start of run, so reset the trigger number
  m_last_trigger_number = 0;

  // Wait for there to be a valid timestamp estimate before we start
  if (m_timestamp_estimator->wait_for_valid_timestamp(m_running_flag) ==
      timinglibs::TimestampEstimatorBase::kInterrupted) {
    return;
  }

  dfmessages::timestamp_t ts = m_timestamp_estimator->get_timestamp_estimate();

  // Round up to the next multiple of trigger_interval_ticks
  dfmessages::timestamp_t next_trigger_timestamp =
    (ts / m_conf.trigger_interval_ticks + 1) * m_conf.trigger_interval_ticks;
  TLOG_DEBUG(1) << "Initial timestamp estimate is " << ts << ", next_trigger_timestamp is " << next_trigger_timestamp;

  assert(next_trigger_timestamp > ts);

  while (m_running_flag.load()) {
    if (m_timestamp_estimator->wait_for_timestamp(next_trigger_timestamp, m_running_flag) ==
        timinglibs::TimestampEstimatorBase::kInterrupted) {
      break;
    }

    triggeralgs::TriggerCandidate candidate = create_candidate(next_trigger_timestamp);

    for (int i = 0; i < m_conf.repeat_trigger_count; ++i) {
      TLOG_DEBUG(1) << "At timestamp " << m_timestamp_estimator->get_timestamp_estimate()
                    << ", pushing a candidate with timestamp " << candidate.time_candidate;
      m_trigger_candidate_sink->push(candidate, std::chrono::milliseconds(10));
      m_last_trigger_number++;
    }

    next_trigger_timestamp += m_conf.trigger_interval_ticks;
  }

  // We get here after the stop command is received. We send out
  // m_stop_burst_count triggers in one go, so that there are triggers
  // in-flight in the system during the stopping transition. This is
  // intended to allow tests that all of the queues are correctly
  // drained elsewhere in the system during the stop transition
  if (m_conf.stop_burst_count) {
    TLOG_DEBUG(0) << "Sending " << m_conf.stop_burst_count << " candidates at stop";

    triggeralgs::TriggerCandidate candidate = create_candidate(next_trigger_timestamp);

    for (int i = 0; i < m_conf.stop_burst_count; ++i) {
      m_trigger_candidate_sink->push(candidate, std::chrono::milliseconds(10));
      m_last_trigger_number++;
    }
  }
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::IntervalTCCreator)

// Local Variables:
// c-basic-offset: 2
// End:
