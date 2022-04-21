/**
 * @file TCBuffer.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2021.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TCBuffer.hpp"

#include "dfmessages/DataRequest.hpp"
#include "daqdataformats/GeoID.hpp"

#include <string>

namespace dunedaq {
namespace trigger {

TCBuffer::TCBuffer(const std::string& name)
  : DAQModule(name)
  , m_thread(std::bind(&TCBuffer::do_work, this, std::placeholders::_1))
  , m_queue_timeout(100)
{

  register_command("conf", &TCBuffer::do_conf);
  register_command("start", &TCBuffer::do_start);
  register_command("stop", &TCBuffer::do_stop);
  register_command("scrap", &TCBuffer::do_scrap);
}

void
TCBuffer::init(const nlohmann::json& init_data)
{
  try {
    m_input_queue_tcs.reset(new tcs_source_t(appfwk::queue_inst(init_data, "tc_source")));
    m_input_queue_dr.reset(new dr_source_t(appfwk::queue_inst(init_data, "data_request_source")));
    m_output_queue_frag.reset(new fragment_sink_t(appfwk::queue_inst(init_data, "fragment_sink")));
  } catch (const ers::Issue& excpt) {
    throw dunedaq::trigger::InvalidQueueFatalError(ERS_HERE, get_name(), "input/output", excpt);
  }
  m_error_registry = std::make_unique<readoutlibs::FrameErrorRegistry>();
  m_latency_buffer_impl = std::make_unique<latency_buffer_t>();
  m_request_handler_impl = std::make_unique<request_handler_t>(m_latency_buffer_impl, m_error_registry);
  m_request_handler_impl->init(init_data);
}

void
TCBuffer::get_info(opmonlib::InfoCollector& /* ci */, int /*level*/)
{
}

void
TCBuffer::do_conf(const nlohmann::json& args)
{
  // Configure the latency buffer before the request handler so the request handler can check for alignment
  // restrictions

  m_latency_buffer_impl->conf(args);
  m_request_handler_impl->conf(args);

  TLOG_DEBUG(2) << get_name() + " configured.";
}

void
TCBuffer::do_start(const nlohmann::json& args)
{
  m_request_handler_impl->start(args);
  m_thread.start_working_thread("tcbuffer");
  TLOG_DEBUG(2) << get_name() + " successfully started.";
}

void
TCBuffer::do_stop(const nlohmann::json& args)
{
  m_thread.stop_working_thread();
  m_request_handler_impl->stop(args);
  m_latency_buffer_impl->flush();
  TLOG_DEBUG(2) << get_name() + " successfully stopped.";
}

void
TCBuffer::do_scrap(const nlohmann::json& args)
{
    m_request_handler_impl->scrap(args);
    m_latency_buffer_impl->scrap(args);
}

void
TCBuffer::do_work(std::atomic<bool>& running_flag)
{
  size_t n_tcs_received = 0;
  size_t n_requests_received = 0;
  
  while (running_flag.load()) {
    
    bool popped_anything=false;
    
    try {
      triggeralgs::TriggerCandidate tc;
      m_input_queue_tcs->pop(tc);
      TLOG_DEBUG(2) << "Got TC with start time " << tc.time_start;
      popped_anything = true;
      m_latency_buffer_impl->write(TCWrapper(tc));
      ++n_tcs_received;
    } catch (const appfwk::QueueTimeoutExpired&) {
      // It's fine if there was no new input
    }

    try {
      dfmessages::DataRequest data_request;
      m_input_queue_dr->pop(data_request);
      auto& info = data_request.request_information;
      TLOG_DEBUG(2) << "Got data request with component " << info.component << ", window_begin " << info.window_begin << ", window_end " << info.window_end;
      popped_anything = true;
      ++n_requests_received;
      m_request_handler_impl->issue_request(data_request, *m_output_queue_frag);
    } catch (const appfwk::QueueTimeoutExpired&) {
      // It's fine if there was no new input
    }

    if (!popped_anything) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  } // while (running_flag.load())

  TLOG() << get_name() << " exiting do_work() method. Received " << n_tcs_received << " TCs " << " and " << n_requests_received << " data requests";
}
} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TCBuffer)
