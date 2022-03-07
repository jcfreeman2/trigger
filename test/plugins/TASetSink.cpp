/**
 * @file TASetSink.cpp TASetSink class implementation
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TASetSink.hpp"

#include "logging/Logging.hpp"

#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/app/Nljs.hpp"
#include "triggeralgs/Types.hpp"
#include <chrono>
#include <sstream>

namespace dunedaq {
namespace trigger {

TASetSink::TASetSink(const std::string& name)
  : DAQModule(name)
{
  register_command("start", &TASetSink::do_start);
  register_command("stop", &TASetSink::do_stop);
  register_command("conf", &TASetSink::do_conf);
}

void
TASetSink::init(const nlohmann::json& obj)
{
  m_taset_source.reset(new source_t(appfwk::queue_inst(obj, "taset_source")));
}

void
TASetSink::do_start(const nlohmann::json& /*obj*/)
{
  m_running_flag.store(true);
  m_thread = std::thread(&TASetSink::do_work, this);
  pthread_setname_np(m_thread.native_handle(), "tasink");
}

void
TASetSink::do_stop(const nlohmann::json& /*obj*/)
{
  m_running_flag.store(false);
  m_thread.join();
}

void
TASetSink::do_conf(const nlohmann::json& obj)
{
  m_conf=obj;
  if (m_conf.output_filename != "") {
    m_outfile.open(m_conf.output_filename);
  }
  else {
    TLOG() << "Output filename is null, so not opening an output file";
  }
}


void
TASetSink::do_work()
{
  using namespace std::chrono;

  size_t n_taset_received = 0;

  auto start_time = steady_clock::now();

  triggeralgs::timestamp_t first_timestamp = 0;
  triggeralgs::timestamp_t last_timestamp = 0;

  uint32_t last_seqno = 0;

  while (true) {
    TASet taset;
    try {
      m_taset_source->pop(taset, std::chrono::milliseconds(100));
      ++n_taset_received;
    } catch (appfwk::QueueTimeoutExpired&) {
      // The condition to exit the loop is that we've been stopped and
      // there's nothing left on the input queue
      if (!m_running_flag.load()) {
        break;
      } else {
        continue;
      }
    }

    if (m_outfile.is_open()) {
      for (auto const& ta : taset.objects) {
        m_outfile << ta.time_start << "\t" << ta.time_end << "\t" << ta.channel_start << "\t" << ta.channel_end << "\t"
                  << ta.adc_integral << std::endl;
        for (auto const& tp : ta.inputs) {
          m_outfile << "\t" << tp.time_start << "\t" << tp.time_over_threshold << "\t" << tp.time_peak << "\t"
                    << tp.channel << "\t" << tp.adc_integral << "\t" << tp.adc_peak << "\t" << tp.detid << "\t"
                    << tp.type << "\t" << std::endl;
        }
        m_outfile << std::endl;
      }
    } // end if(m_outfile.is_open())

    if (m_conf.do_checks) {
      // Do some checks on the received TASet
      if (last_seqno != 0 && taset.seqno != last_seqno + 1) {
        TLOG() << "Missed TASets: last_seqno=" << last_seqno << ", current seqno=" << taset.seqno;
      }

      if (taset.start_time < last_timestamp) {
        TLOG_DEBUG(1) << "TASets out of order: last start time " << last_timestamp << ", current start time "
                      << taset.start_time;
      }
      if (taset.type == TASet::Type::kHeartbeat) {
        TLOG_DEBUG(1) << "Heartbeat TASet with start time " << taset.start_time;
      } else if (taset.objects.empty()) {
        TLOG_DEBUG(1) << "Empty TASet with start time " << taset.start_time;
      }
      for (auto const& tp : taset.objects) {
        if (tp.time_start < taset.start_time || tp.time_start > taset.end_time) {
          TLOG() << "TASet with start time " << taset.start_time << ", end time " << taset.end_time
                 << " contains out-of-bounds TP with start time " << tp.time_start;
        }
      }
    } // end if(m_conf.do_checks)

    last_seqno = taset.seqno;
    
    if (first_timestamp == 0) {
      first_timestamp = taset.start_time;
    }
    last_timestamp = taset.start_time;
  } // while(true)

  auto end_time = steady_clock::now();
  auto time_ms = duration_cast<milliseconds>(end_time - start_time).count();
  float rate_hz = 1e3 * static_cast<float>(n_taset_received) / time_ms;
  float inferred_clock_frequency = 1e3 * (last_timestamp - first_timestamp) / time_ms;

  TLOG() << "Received " << n_taset_received << " TASets in " << time_ms << "ms. " << rate_hz
         << " TASet/s. Inferred clock frequency " << inferred_clock_frequency << "Hz";
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TASetSink)
