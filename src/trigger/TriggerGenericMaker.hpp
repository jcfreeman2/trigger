/**
 * @file TriggerGenericMaker.hpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_SRC_TRIGGER_TRIGGERGENERICMAKER_HPP_
#define TRIGGER_SRC_TRIGGER_TRIGGERGENERICMAKER_HPP_

#include "trigger/Issues.hpp"
#include "trigger/Set.hpp"
#include "trigger/TimeSliceInputBuffer.hpp"
#include "trigger/TimeSliceOutputBuffer.hpp"

#include "appfwk/DAQModule.hpp"
#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/DAQSink.hpp"
#include "appfwk/DAQSource.hpp"
#include "appfwk/ThreadHelper.hpp"

#include "daqdataformats/GeoID.hpp"

#include "logging/Logging.hpp"
#include "triggeralgs/Types.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace dunedaq::trigger {

// Forward declare the class encapsulating partial specifications of do_work
template<class IN, class OUT, class MAKER>
class TriggerGenericWorker;

// This template class reads IN items from queues, passes them to MAKER objects,
// and writes the resulting OUT objects to another queue. The behavior of
// passing IN objects to the MAKER and creating OUT objects from the MAKER is
// encapsulated by TriggerGenericWorker<IN,OUT,MAKER> templates, defined later
// in this file
template<class IN, class OUT, class MAKER>
class TriggerGenericMaker : public dunedaq::appfwk::DAQModule
{
  friend class TriggerGenericWorker<IN, OUT, MAKER>;

public:
  explicit TriggerGenericMaker(const std::string& name)
    : DAQModule(name)
    , m_thread(std::bind(&TriggerGenericMaker::do_work, this, std::placeholders::_1))
    , m_input_queue(nullptr)
    , m_output_queue(nullptr)
    , m_queue_timeout(100)
    , m_algorithm_name("[uninitialized]")
    , m_geoid_region_id(dunedaq::daqdataformats::GeoID::s_invalid_region_id)
    , m_geoid_element_id(dunedaq::daqdataformats::GeoID::s_invalid_element_id)
    , m_buffer_time(0)
    , m_window_time(625000)
    , worker(*this) // should be last; may use other members
  {
    register_command("start", &TriggerGenericMaker::do_start);
    register_command("stop", &TriggerGenericMaker::do_stop);
    register_command("conf", &TriggerGenericMaker::do_configure);
  }

  virtual ~TriggerGenericMaker() {}

  TriggerGenericMaker(const TriggerGenericMaker&) = delete;
  TriggerGenericMaker& operator=(const TriggerGenericMaker&) = delete;
  TriggerGenericMaker(TriggerGenericMaker&&) = delete;
  TriggerGenericMaker& operator=(TriggerGenericMaker&&) = delete;

  void init(const nlohmann::json& obj) override
  {
    m_input_queue.reset(new source_t(appfwk::queue_inst(obj, "input")));
    m_output_queue.reset(new sink_t(appfwk::queue_inst(obj, "output")));
  }

protected:
  void set_algorithm_name(const std::string& name) { m_algorithm_name = name; }

  // Only applies to makers that output Set<B>
  void set_geoid(uint16_t region_id, uint32_t element_id)
  {
    m_geoid_region_id = region_id;
    m_geoid_element_id = element_id;
  }

  // Only applies to makers that output Set<B>
  void set_windowing(daqdataformats::timestamp_t window_time, daqdataformats::timestamp_t buffer_time)
  {
    m_window_time = window_time;
    m_buffer_time = buffer_time;
  }

private:
  dunedaq::appfwk::ThreadHelper m_thread;

  size_t m_received_count;
  size_t m_sent_count;

  using source_t = dunedaq::appfwk::DAQSource<IN>;
  std::unique_ptr<source_t> m_input_queue;

  using sink_t = dunedaq::appfwk::DAQSink<OUT>;
  std::unique_ptr<sink_t> m_output_queue;

  std::chrono::milliseconds m_queue_timeout;

  std::string m_algorithm_name;

  uint16_t m_geoid_region_id;
  uint32_t m_geoid_element_id;

  daqdataformats::timestamp_t m_buffer_time;
  daqdataformats::timestamp_t m_window_time;

  std::shared_ptr<MAKER> m_maker;

  TriggerGenericWorker<IN, OUT, MAKER> worker;

  // This should return a shared_ptr to the MAKER created from conf command arguments.
  // Should also call set_algorithm_name and set_geoid/set_windowing (if desired)
  virtual std::shared_ptr<MAKER> make_maker(const nlohmann::json& obj) = 0;

  void do_start(const nlohmann::json& /*obj*/)
  {
    m_received_count = 0;
    m_sent_count = 0;
    m_thread.start_working_thread();
  }

  void do_stop(const nlohmann::json& /*obj*/) { m_thread.stop_working_thread(); }

  void do_configure(const nlohmann::json& obj)
  {
    m_maker = make_maker(obj);
    // worker should be notified that configuration potentially changed
    worker.reconfigure();
  }

  void do_work(std::atomic<bool>& running_flag)
  {
    // Loop until a stop is received
    while (running_flag.load()) {
      // While there are items in the input queue, continue draining even if
      // the running_flag is false, but stop _immediately_ when input is empty
      IN in;
      while (receive(in)) {
        worker.process(in);
      }
    }
    worker.drain();
    TLOG() << ": Exiting do_work() method, received " << m_received_count << " inputs and successfully sent "
           << m_sent_count << " outputs. ";
    worker.reset();
  }

  bool receive(IN& in)
  {
    try {
      m_input_queue->pop(in, m_queue_timeout);
    } catch (const dunedaq::appfwk::QueueTimeoutExpired& excpt) {
      // it is perfectly reasonable that there might be no data in the queue
      // some fraction of the times that we check, so we just continue on and try again
      return false;
    }
    ++m_received_count;
    return true;
  }

  bool send(const OUT& out)
  {
    try {
      m_output_queue->push(out, m_queue_timeout);
    } catch (const dunedaq::appfwk::QueueTimeoutExpired& excpt) {
      ers::warning(excpt);
      return false;
    }
    ++m_sent_count;
    return true;
  }
};

// To handle the different unpacking schemes implied by different templates,
// do_work is broken out into its own template class that is a friend of
// TriggerGenericMaker. C++ still does not support partial specification of a
// single method in a template class, so this approach is the least redundant
// way to achieve that functionality

// The base template assumes the MAKER has an operator() with the signature
// operator()(IN, std::vector<OUT>)
template<class IN, class OUT, class MAKER>
class TriggerGenericWorker
{
public:
  explicit TriggerGenericWorker(TriggerGenericMaker<IN, OUT, MAKER>& parent)
    : m_parent(parent)
  {}

  TriggerGenericMaker<IN, OUT, MAKER>& m_parent;

  void reconfigure() {}

  void reset() {}

  void process(IN& in)
  {
    std::vector<OUT> out_vec; // one input -> many outputs
    try {
      m_parent.m_maker->operator()(in, out_vec);
    } catch (...) { // TODO BJL May 28-2021 can we restrict the possible exceptions triggeralgs might raise?
      ers::fatal(AlgorithmFatalError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
      return;
    }

    while (out_vec.size()) {
      if (!m_parent.send(out_vec.back())) {
        ers::error(AlgorithmFailedToSend(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        // out_vec.back() is dropped
      }
      out_vec.pop_back();
    }
  }

  void drain() {}
};

// Partial specialization for IN = Set<A>, OUT = Set<B> and assumes the MAKER has:
// operator()(A, std::vector<B>)
template<class A, class B, class MAKER>
class TriggerGenericWorker<Set<A>, Set<B>, MAKER>
{
public:
  explicit TriggerGenericWorker(TriggerGenericMaker<Set<A>, Set<B>, MAKER>& parent)
    : m_parent(parent)
    , m_in_buffer(parent.get_name(), parent.m_algorithm_name)
    , m_out_buffer(parent.get_name(), parent.m_algorithm_name, parent.m_buffer_time)
  {}

  TriggerGenericMaker<Set<A>, Set<B>, MAKER>& m_parent;

  TimeSliceInputBuffer<A> m_in_buffer;
  TimeSliceOutputBuffer<B> m_out_buffer;

  daqdataformats::timestamp_t m_prev_start_time = 0;

  void reconfigure()
  {
    m_out_buffer.set_window_time(m_parent.m_window_time);
    m_out_buffer.set_buffer_time(m_parent.m_buffer_time);
  }

  void reset()
  {
    m_prev_start_time = 0;
    m_out_buffer.reset();
  }

  void process_slice(const std::vector<A>& time_slice, std::vector<B>& out_vec)
  {
    // time_slice is a full slice (all Set<A> combined), time ordered, vector of A
    // call operator for each of the objects in the vector
    for (const A& x : time_slice) {
      try {
        m_parent.m_maker->operator()(x, out_vec);
      } catch (...) { // TODO BJL May 28-2021 can we restrict the possible exceptions triggeralgs might raise?
        ers::fatal(AlgorithmFatalError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        return;
      }
    }
  }

  void process(Set<A>& in)
  {
    std::vector<B> elems; // Bs to buffer for the next window
    switch (in.type) {
      case Set<A>::Type::kPayload: {
        if (m_prev_start_time != 0 && in.start_time < m_prev_start_time) {
          ers::warning(OutOfOrderSets(ERS_HERE, m_parent.get_name(), m_prev_start_time, in.start_time));
        }
        m_prev_start_time = in.start_time;
        std::vector<A> time_slice;
        daqdataformats::timestamp_t start_time, end_time;
        if (!m_in_buffer.buffer(in, time_slice, start_time, end_time)) {
          return; // no complete time slice yet (`in` was part of buffered slice)
        }
        process_slice(time_slice, elems);
      } break;
      case Set<A>::Type::kHeartbeat: {
        // PAR 2022-01-21 We've got a heartbeat for time T, so we know
        // we won't receive any more inputs for times t < T. Therefore
        // we can flush all items in the input buffer, which have
        // times t < T, because the input is time-ordered. We also
        // forward the heartbeat downstream

        std::vector<A> time_slice;
        daqdataformats::timestamp_t start_time, end_time;
        if (m_in_buffer.flush(time_slice, start_time, end_time)) {
          if (end_time > in.start_time) {
            // This should never happen, but we check here so we at least get some output if it did
            ers::fatal(OutOfOrderSets(ERS_HERE, m_parent.get_name(), end_time, in.start_time));
          }
          process_slice(time_slice, out_vec);
        }
        
        Set<B> heartbeat;
        heartbeat.seqno = m_parent.m_sent_count;
        heartbeat.type = Set<B>::Type::kHeartbeat;
        heartbeat.start_time = in.start_time;
        heartbeat.end_time = in.end_time;
        heartbeat.origin = daqdataformats::GeoID(
          daqdataformats::GeoID::SystemType::kDataSelection, m_parent.m_geoid_region_id, m_parent.m_geoid_element_id);
        if (!m_parent.send(heartbeat)) {
          ers::error(AlgorithmFailedToHeartbeat(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          // heartbeat is dropped
        }

        // flush the maker
        try {
          // TODO BJL July 14-2021 flushed events go into the buffer... until a window is ready?
          m_parent.m_maker->flush(in.end_time, elems);
        } catch (...) { // TODO BJL May 28-2021 can we restrict the possible exceptions triggeralgs might raise?
          ers::fatal(AlgorithmFatalError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          return;
        }
      } break;
      case Set<A>::Type::kUnknown:
        ers::error(UnknownSetError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        break;
    }

    // add new elements to output buffer
    if (elems.size() > 0) {
      m_out_buffer.buffer(elems);
    }

    // emit completed windows
    while (m_out_buffer.ready()) {
      Set<B> out;
      m_out_buffer.flush(out.objects, out.start_time, out.end_time);
      // Only form and send Set<B> if it has a nonzero number of objects
      if (out.objects.size() != 0) {
        out.seqno = m_parent.m_sent_count;
        out.type = Set<B>::Type::kPayload;
        out.origin = daqdataformats::GeoID(
          daqdataformats::GeoID::SystemType::kDataSelection, m_parent.m_geoid_region_id, m_parent.m_geoid_element_id);
        TLOG_DEBUG(2) << "Output set window ready with start time " << out.start_time << " end time " << out.end_time
                      << " and " << out.objects.size() << " members";
        if (!m_parent.send(out)) {
          ers::error(AlgorithmFailedToSend(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          // out is dropped
        }
      }
    }
  }

  void drain()
  {
    // First, send anything in the input buffer to the algorithm, and add any
    // results to output buffer
    std::vector<A> time_slice;
    daqdataformats::timestamp_t start_time, end_time;
    if (m_in_buffer.flush(time_slice, start_time, end_time)) {
      std::vector<B> elems;
      process_slice(time_slice, elems);
      if (elems.size() > 0) {
        m_out_buffer.buffer(elems);
      }
    }
    // Second, drain the output buffer onto the queue. These may not be "fully
    // formed" windows, but at this point we're getting no more data anyway.
    while (!m_out_buffer.empty()) {
      Set<B> out;
      m_out_buffer.flush(out.objects, out.start_time, out.end_time);
      // Only form and send Set<B> if it has a nonzero number of objects
      if (out.objects.size() != 0) {
        out.seqno = m_parent.m_sent_count;
        out.type = Set<B>::Type::kPayload;
        out.origin = daqdataformats::GeoID(
          daqdataformats::GeoID::SystemType::kDataSelection, m_parent.m_geoid_region_id, m_parent.m_geoid_element_id);
        TLOG_DEBUG(2) << "Output set window drained with start time " << out.start_time << " end time " << out.end_time
                      << " and " << out.objects.size() << " members";
        if (!m_parent.send(out)) {
          ers::error(AlgorithmFailedToSend(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          // out is dropped
        }
      }
    }
  }
};

// Partial specialization for IN = Set<A> and assumes the the MAKER has:
// operator()(A, std::vector<OUT>)
template<class A, class OUT, class MAKER>
class TriggerGenericWorker<Set<A>, OUT, MAKER>
{
public:
  explicit TriggerGenericWorker(TriggerGenericMaker<Set<A>, OUT, MAKER>& parent)
    : m_parent(parent)
    , m_in_buffer(parent.get_name(), parent.m_algorithm_name)
  {}

  TriggerGenericMaker<Set<A>, OUT, MAKER>& m_parent;

  TimeSliceInputBuffer<A> m_in_buffer;

  void reconfigure() {}

  void reset() {}

  void process_slice(const std::vector<A>& time_slice, std::vector<OUT>& out_vec)
  {
    // time_slice is a full slice (all Set<A> combined), time ordered, vector of A
    // call operator for each of the objects in the vector
    for (const A& x : time_slice) {
      try {
        m_parent.m_maker->operator()(x, out_vec);
      } catch (...) { // TODO BJL May 28-2021 can we restrict the possible exceptions triggeralgs might raise?
        ers::fatal(AlgorithmFatalError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        return;
      }
    }
  }

  void process(Set<A>& in)
  {
    std::vector<OUT> out_vec; // either a whole time slice, heartbeat flushed, or empty
    switch (in.type) {
      case Set<A>::Type::kPayload: {
        std::vector<A> time_slice;
        daqdataformats::timestamp_t start_time, end_time;
        if (!m_in_buffer.buffer(in, time_slice, start_time, end_time)) {
          return; // no complete time slice yet (`in` was part of buffered slice)
        }
        process_slice(time_slice, out_vec);
      } break;
      case Set<A>::Type::kHeartbeat:
        // TODO BJL May-28-2021 should anything happen with the heartbeat when OUT is not a Set<T>?
        //
        // PAR 2022-01-21 We've got a heartbeat for time T, so we know
        // we won't receive any more inputs for times t < T. Therefore
        // we can flush all items in the input buffer, which have
        // times t < T, because the input is time-ordered
        try {
          std::vector<A> time_slice;
          daqdataformats::timestamp_t start_time, end_time;
          if (m_in_buffer.flush(time_slice, start_time, end_time)) {
            if (end_time > in.start_time) {
              // This should never happen, but we check here so we at least get some output if it did
              ers::fatal(OutOfOrderSets(ERS_HERE, m_parent.get_name(), end_time, in.start_time));
            }
            process_slice(time_slice, out_vec);
          }
          m_parent.m_maker->flush(in.end_time, out_vec);
        } catch (...) { // TODO BJL May 28-2021 can we restrict the possible exceptions triggeralgs might raise?
          ers::fatal(AlgorithmFatalError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          return;
        }
        break;
      case Set<A>::Type::kUnknown:
        ers::error(UnknownSetError(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        break;
    }

    while (out_vec.size()) {
      if (!m_parent.send(out_vec.back())) {
        ers::error(AlgorithmFailedToSend(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
        // out.back() is dropped
      }
      out_vec.pop_back();
    }
  }

  void drain()
  {
    // Send anything in the input buffer to the algorithm, and put any results
    // on the output queue
    std::vector<A> time_slice;
    daqdataformats::timestamp_t start_time, end_time;
    if (m_in_buffer.flush(time_slice, start_time, end_time)) {
      std::vector<OUT> out_vec;
      process_slice(time_slice, out_vec);
      while (out_vec.size()) {
        if (!m_parent.send(out_vec.back())) {
          ers::error(AlgorithmFailedToSend(ERS_HERE, m_parent.get_name(), m_parent.m_algorithm_name));
          // out.back() is dropped
        }
        out_vec.pop_back();
      }
    }
  }
};

} // namespace dunedaq::trigger

#endif // TRIGGER_SRC_TRIGGER_TRIGGERGENERICMAKER_HPP_
