/**
 * @file TPSetReceiver.cpp TPSetReceiver class implementation
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TPSetReceiver.hpp"
#include "trigger/Issues.hpp"
#include "trigger/tpsetreceiver/Nljs.hpp"
#include "trigger/tpsetreceiver/Structs.hpp"
#include "trigger/tpsetreceiverinfo/InfoNljs.hpp"

#include "appfwk/DAQModuleHelper.hpp"
#include "appfwk/app/Nljs.hpp"
#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

/**
 * @brief Name used by TRACE TLOG calls from this source file
 */
#define TRACE_NAME "TPSetReceiver" // NOLINT
enum
{
  TLVL_CONFIG = 7,
  TLVL_WORK_STEPS = 10
};

namespace dunedaq {
namespace trigger {

TPSetReceiver::TPSetReceiver(const std::string& name)
  : dunedaq::appfwk::DAQModule(name)
  , m_queue_timeout(100)
  , m_tpset_output_queues()
{
  register_command("conf", &TPSetReceiver::do_conf);
  register_command("start", &TPSetReceiver::do_start);
  register_command("stop", &TPSetReceiver::do_stop);
  register_command("scrap", &TPSetReceiver::do_scrap);
}

void
TPSetReceiver::init(const data_t& init_data)
{
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering init() method";

  //----------------------
  // Get dynamic queues
  //----------------------

  // set names for the tpset queue(s)
  auto ini = init_data.get<appfwk::app::ModInit>();

  // Test for valid output tpset queues
  for (const auto& qitem : ini.qinfos) {
    if (qitem.name.rfind("tpset_q_for_buf") == 0) {
      try {
        tpsetsink_t temp(qitem.inst);
      } catch (const ers::Issue& excpt) {
        throw InvalidQueueFatalError(ERS_HERE, get_name(), qitem.name, excpt);
      }
    }
  }

  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting init() method";
}

void
TPSetReceiver::do_conf(const data_t& payload)
{
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_conf() method";

  m_tpset_output_queues.clear();

  tpsetreceiver::ConfParams parsed_conf = payload.get<tpsetreceiver::ConfParams>();

  for (auto const& entry : parsed_conf.map) {

    daqdataformats::GeoID::SystemType type = daqdataformats::GeoID::string_to_system_type(entry.system);

    if (type == daqdataformats::GeoID::SystemType::kInvalid) {
      throw InvalidSystemType(ERS_HERE, entry.system);
    }

    daqdataformats::GeoID key;
    key.system_type = type;
    key.region_id = entry.region;
    key.element_id = entry.element;
    m_tpset_output_queues[key] = std::unique_ptr<tpsetsink_t>(new tpsetsink_t(entry.queueinstance));
  }

  m_queue_timeout = std::chrono::milliseconds(parsed_conf.general_queue_timeout);
  m_topic = parsed_conf.topic;
  TLOG_DEBUG(TLVL_CONFIG) << "Topic name is " << m_topic;

  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_conf() method";
}

void
TPSetReceiver::do_start(const data_t&)
{
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_start() method";

  m_received_tpsets = 0;

  // Subscribe here so that we know publishers have been started.
  iomanager::IOManager iom;
  iom.get_receiver<trigger::TPSet>(m_topic).add_callback(
    std::bind(&TPSetReceiver::dispatch_tpset, this, std::placeholders::_1));

  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_start() method";
}

void
TPSetReceiver::do_stop(const data_t& /*args*/)
{
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_stop() method";

  iomanager::IOManager iom;
  iom.remove_callback<trigger::TPSet>(m_topic);

  TLOG() << get_name() << " successfully stopped";
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_stop() method";
}

void
TPSetReceiver::do_scrap(const data_t& /*args*/)
{
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Entering do_scrap() method";

  TLOG() << get_name() << " successfully stopped";
  TLOG_DEBUG(TLVL_ENTER_EXIT_METHODS) << get_name() << ": Exiting do_scrap() method";
}

void
TPSetReceiver::get_info(opmonlib::InfoCollector& ci, int /*level*/)
{
  tpsetreceiverinfo::Info info;
  info.tpsets_received = m_received_tpsets;
  ci.add(info);
}

void
TPSetReceiver::dispatch_tpset(trigger::TPSet& tpset)
{
  TLOG_DEBUG(10) << get_name() << "Received tpset: " << tpset.seqno << ", ts=" << tpset.start_time << "-"
                 << tpset.end_time;

  auto component = tpset.origin;
  if (m_tpset_output_queues.count(component)) {
    TLOG_DEBUG(10) << get_name() << "Dispatch tpset to queue " << m_tpset_output_queues.at(component)->get_name();
    try {
      m_tpset_output_queues.at(component)->push(tpset, m_queue_timeout);
    } catch (const dunedaq::iomanager::TimeoutExpired& excpt) {
      std::ostringstream oss_warn;
      oss_warn << "push to output queue \"" << m_tpset_output_queues.at(component)->get_name() << "\"";
      ers::warning(dunedaq::iomanager::TimeoutExpired(ERS_HERE, get_name(), oss_warn.str(), m_queue_timeout.count()));
    }
  } else {
    ers::error(UnknownGeoID(ERS_HERE, component));
  }
  m_received_tpsets++;
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TPSetReceiver)
