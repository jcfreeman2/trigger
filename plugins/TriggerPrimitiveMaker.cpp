/**
 * @file TriggerPrimitiveMaker.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TriggerPrimitiveMaker.hpp"

#include "trigger/Issues.hpp" // For TLVL_*

#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/cmd/Nljs.hpp"
#include "rcif/cmd/Nljs.hpp"

#include "logging/Logging.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

using namespace triggeralgs;

namespace dunedaq::trigger {

TriggerPrimitiveMaker::TriggerPrimitiveMaker(const std::string& name)
  : dunedaq::appfwk::DAQModule(name)
  , m_thread(std::bind(&TriggerPrimitiveMaker::do_work, this, std::placeholders::_1))
  , m_tpset_sink()
  , m_queue_timeout(100)
{
  register_command("conf", &TriggerPrimitiveMaker::do_configure);
  register_command("start", &TriggerPrimitiveMaker::do_start);
  register_command("stop", &TriggerPrimitiveMaker::do_stop);
  register_command("scrap", &TriggerPrimitiveMaker::do_scrap);
}

void
TriggerPrimitiveMaker::init(const nlohmann::json& obj)
{
  m_tpset_sink.reset(new appfwk::DAQSink<TPSet>(appfwk::queue_inst(obj, "tpset_sink")));
}

void
TriggerPrimitiveMaker::do_configure(const nlohmann::json& obj)
{
  m_conf = obj.get<triggerprimitivemaker::ConfParams>();

  std::ifstream file(m_conf.filename);
  if (!file || file.bad()) {
    throw BadTPInputFile(ERS_HERE, get_name(), m_conf.filename);
  }

  TriggerPrimitive tp;
  TPSet tpset;

  uint64_t prev_tpset_number = 0; // NOLINT(build/unsigned)
  uint32_t seqno = 0;             // NOLINT(build/unsigned)
  uint64_t old_time_start = 0;    // NOLINT(build/unsigned)

  // Read in the file and place the TPs in TPSets. TPSets have time
  // boundaries ( n*tpset_time_width + tpset_time_offset ), and TPs are placed
  // in TPSets based on the TP start time
  //
  // This loop assumes the input file is sorted by TP start time
  while (file >> tp.time_start >> tp.time_over_threshold >> tp.time_peak >> tp.channel >> tp.adc_integral >>
         tp.adc_peak >> tp.detid >> tp.type) {
    if (tp.time_start >= old_time_start) {
      // NOLINTNEXTLINE(build/unsigned)
      uint64_t current_tpset_number = (tp.time_start + m_conf.tpset_time_offset) / m_conf.tpset_time_width;
      old_time_start = tp.time_start;

      // If we crossed a time boundary, push the current TPSet and reset it
      if (current_tpset_number > prev_tpset_number) {
        if (!tpset.objects.empty()) {
          // We don't send empty TPSets, so there's no point creating them
          m_tpsets.push_back(tpset);
        }
        prev_tpset_number = current_tpset_number;

        tpset.start_time = current_tpset_number * m_conf.tpset_time_width + m_conf.tpset_time_offset;
        tpset.end_time = tpset.start_time + m_conf.tpset_time_width;
        tpset.seqno = seqno;
        ++seqno;

        // 12-Jul-2021, KAB: setting origin fields from configuration
        tpset.origin.region_id = m_conf.region_id;
        tpset.origin.element_id = m_conf.element_id;

        tpset.type = TPSet::Type::kPayload;
        tpset.objects.clear();
      }
      tpset.objects.push_back(tp);
    } else {
      ers::warning(UnsortedTP(ERS_HERE, get_name(), tp.time_start));
    }
  }
  if (!tpset.objects.empty()) {
    // We don't send empty TPSets, so there's no point creating them
    m_tpsets.push_back(tpset);
  }
}

void
TriggerPrimitiveMaker::do_start(const nlohmann::json& args)
{
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_start() method";
  rcif::cmd::StartParams start_params = args.get<rcif::cmd::StartParams>();
  m_run_number = start_params.run;

  m_thread.start_working_thread("tpmaker");
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_start() method";
}

void
TriggerPrimitiveMaker::do_stop(const nlohmann::json& /*args*/)
{
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_stop() method";
  m_thread.stop_working_thread();
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_stop() method";
}

void
TriggerPrimitiveMaker::do_scrap(const nlohmann::json& /*args*/)
{
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_scrap() method";
  m_tpsets.clear();
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_scrap() method";
}

void
TriggerPrimitiveMaker::do_work(std::atomic<bool>& running_flag)
{
  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_work() method";
  uint64_t current_iteration = 0; // NOLINT(build/unsigned)
  size_t generated_count = 0;
  size_t push_failed_count = 0;
  size_t generated_tp_count = 0;

  uint64_t prev_tpset_start_time = 0; // NOLINT(build/unsigned)
  auto prev_tpset_send_time = std::chrono::steady_clock::now();

  auto input_file_duration = m_tpsets.back().start_time - m_tpsets.front().start_time + m_conf.tpset_time_width;

  auto run_start_time = std::chrono::steady_clock::now();

  uint32_t seqno = 0; // NOLINT(build/unsigned)

  while (running_flag.load()) {
    if (m_conf.number_of_loops > 0 && current_iteration >= m_conf.number_of_loops) {
      break;
    }

    for (auto& tpset : m_tpsets) {

      if (!running_flag.load()) {
        break;
      }

      // We send out the first TPSet right away. After that we space each TPSet in time by their start time
      auto wait_time_us = 0;
      if (prev_tpset_start_time == 0) {
        wait_time_us = 0;
      } else {
        auto clocks_per_us = m_conf.clock_frequency_hz / 1'000'000;
        wait_time_us = (tpset.start_time - prev_tpset_start_time) / clocks_per_us;
      }

      auto next_tpset_send_time = prev_tpset_send_time + std::chrono::microseconds(wait_time_us);
      // check running_flag periodically using lambda
      auto slice_period = std::chrono::microseconds(m_conf.maximum_wait_time_us);
      auto next_slice_send_time = prev_tpset_send_time + slice_period;
      bool break_flag = false;
      while (next_tpset_send_time > next_slice_send_time + slice_period) {
        if (!running_flag.load()) {
          TLOG() << "while waiting to send next TP, negative running flag detected.";
          break_flag = true;
          break;
        }
        std::this_thread::sleep_until(next_slice_send_time);
        next_slice_send_time = next_slice_send_time + slice_period;
      }
      if (break_flag == false) {
        std::this_thread::sleep_until(next_tpset_send_time);
      }
      prev_tpset_send_time = next_tpset_send_time;
      prev_tpset_start_time = tpset.start_time;

      ++generated_count;
      generated_tp_count += tpset.objects.size();
      try {
        m_tpset_sink->push(tpset, m_queue_timeout);
      } catch (const dunedaq::appfwk::QueueTimeoutExpired& e) {
        ers::warning(e);
        ++push_failed_count;
      }

      tpset.run_number = m_run_number;
      // Increase seqno and the timestamps in the TPSet and TPs so they don't
      // repeat when we do multiple loops over the file
      tpset.start_time += input_file_duration;
      tpset.end_time += input_file_duration;
      for (auto& tp : tpset.objects) {
        tp.time_start += input_file_duration;
        tp.time_peak += input_file_duration;
      }
      tpset.seqno = seqno;
      ++seqno;

    } // end loop over tpsets
    ++current_iteration;

  } // end while(running_flag.load())

  auto run_end_time = std::chrono::steady_clock::now();
  auto time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(run_end_time - run_start_time).count();
  float rate_hz = 1e3 * static_cast<float>(generated_count) / time_ms;

  TLOG() << "Generated " << generated_count << " TP sets (" << generated_tp_count << " TPs) in " << time_ms << " ms. ("
         << rate_hz << " TPSets/s). " << push_failed_count << " failed to push";

  TLOG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_work() method";
}

} // namespace dunedaq::trigger

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TriggerPrimitiveMaker)
