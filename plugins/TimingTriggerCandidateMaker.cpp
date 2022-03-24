/**
 * @file TimingTriggerCandidateMaker.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TimingTriggerCandidateMaker.hpp"
#include "detdataformats/trigger/Types.hpp"
#include "networkmanager/NetworkManager.hpp"

#include <string>
#include <regex>

namespace dunedaq {
namespace trigger {

TimingTriggerCandidateMaker::TimingTriggerCandidateMaker(const std::string& name)
  : DAQModule(name)
  , m_output_queue(nullptr)
  , m_queue_timeout(100)
{

  register_command("conf", &TimingTriggerCandidateMaker::do_conf);
  register_command("start", &TimingTriggerCandidateMaker::do_start);
  register_command("stop", &TimingTriggerCandidateMaker::do_stop);
  register_command("scrap", &TimingTriggerCandidateMaker::do_scrap);
}

triggeralgs::TriggerCandidate
TimingTriggerCandidateMaker::HSIEventToTriggerCandidate(const dfmessages::HSIEvent& data)
{
  triggeralgs::TriggerCandidate candidate;
  // TODO Trigger Team <dune-daq@github.com> Nov-18-2021: the signal field ia now a signal bit map, rather than unique value -> change logic of below?
  if (m_hsi_passthrough == true){
    TLOG_DEBUG(3) << "HSI passthrough applied, modified readout window is set";
    candidate.time_start = data.timestamp - hsi_pt_before;
    candidate.time_end   = data.timestamp + hsi_pt_after;
  } else {
    if (m_detid_offsets_map.count(data.signal_map)) {
      // clang-format off
      candidate.time_start = data.timestamp - m_detid_offsets_map[data.signal_map].first;  // time_start
      candidate.time_end   = data.timestamp + m_detid_offsets_map[data.signal_map].second; // time_end,
      // clang-format on    
    } else {
      throw dunedaq::trigger::SignalTypeError(ERS_HERE, get_name(), data.signal_map);
    }
  }
  candidate.time_candidate = data.timestamp;
  // throw away bits 31-16 of header, that's OK for now
  candidate.detid = { static_cast<triggeralgs::detid_t>(data.signal_map) }; // NOLINT(build/unsigned)
  candidate.type = triggeralgs::TriggerCandidate::Type::kTiming;

  candidate.algorithm = triggeralgs::TriggerCandidate::Algorithm::kHSIEventToTriggerCandidate;
  candidate.inputs = {};

  return candidate;
}

std::bitset<16>
TimingTriggerCandidateMaker::MakeBitmask16(uint16_t signal_map)
{
  std::bitset<16> bitmask = std::bitset<16>(signal_map);
  return bitmask;
}

std::bitset<8>
TimingTriggerCandidateMaker::MakeBitmask8(unsigned short signal_map)
{
  std::bitset<8> bitmask = std::bitset<8>(signal_map);
  return bitmask;
}

std::vector< std::bitset<8> >
TimingTriggerCandidateMaker::SplitBits(uint16_t signal_map)
{
  unsigned short low = signal_map & 0xff;
  unsigned short high = (signal_map >> 8) & 0xff;
  std::bitset<8> low_mask = MakeBitmask8(low);
  std::bitset<8> high_mask = MakeBitmask8(high); 
  std::vector< std::bitset<8> > LowHighBits;
  LowHighBits.push_back( low_mask );
  LowHighBits.push_back( high_mask );  
  return LowHighBits; 
}

void
TimingTriggerCandidateMaker::AnyBitSet(std::bitset<8> bitmap)
{
  bool BitsSet = bitmap.any();
  if (BitsSet == true){
    throw dunedaq::trigger::BadTriggerBitmask(ERS_HERE, get_name(), bitmap);
  }
  return;
}

void
TimingTriggerCandidateMaker::do_conf(const nlohmann::json& config)
{
  auto params = config.get<dunedaq::trigger::timingtriggercandidatemaker::Conf>();
  m_detid_offsets_map[params.s0.signal_type] = { params.s0.time_before, params.s0.time_after };
  m_detid_offsets_map[params.s1.signal_type] = { params.s1.time_before, params.s1.time_after };
  m_detid_offsets_map[params.s2.signal_type] = { params.s2.time_before, params.s2.time_after };
  m_hsievent_receive_connection = params.hsievent_connection_name;
  m_hsi_passthrough = params.hsi_trigger_type_passthrough;
  hsi_pt_before = params.s0.time_before;
  hsi_pt_after = params.s0.time_after;
  TLOG_DEBUG(2) << get_name() + " configured.";
}

void
TimingTriggerCandidateMaker::init(const nlohmann::json& iniobj)
{
  try {
    m_output_queue.reset(new sink_t(appfwk::queue_inst(iniobj, "output")));
  } catch (const ers::Issue& excpt) {
    throw dunedaq::trigger::InvalidQueueFatalError(ERS_HERE, get_name(), "input/output", excpt);
  }
}

void
TimingTriggerCandidateMaker::do_start(const nlohmann::json&)
{
  // OpMon.
  m_tsd_received_count.store(0);
  m_tc_sent_count.store(0);
  m_tc_sig_type_err_count.store(0);
  m_tc_total_count.store(0);
  
  networkmanager::NetworkManager::get().start_listening(m_hsievent_receive_connection);
  networkmanager::NetworkManager::get().register_callback(
    m_hsievent_receive_connection, std::bind(&TimingTriggerCandidateMaker::receive_hsievent, this, std::placeholders::_1));

  TLOG_DEBUG(2) << get_name() + " successfully started.";
}

void
TimingTriggerCandidateMaker::do_stop(const nlohmann::json&)
{
  networkmanager::NetworkManager::get().clear_callback(m_hsievent_receive_connection);
  networkmanager::NetworkManager::get().stop_listening(m_hsievent_receive_connection);

  TLOG() << "Received " << m_tsd_received_count << " HSIEvent messages. Successfully sent " << m_tc_sent_count
         << " TriggerCandidates";
  TLOG_DEBUG(2) << get_name() + " successfully stopped.";
}

void
TimingTriggerCandidateMaker::receive_hsievent(ipm::Receiver::Response message)
{
  TLOG_DEBUG(3) << "Activity received.";
  
  auto data = serialization::deserialize<dfmessages::HSIEvent>(message.data);
  ++m_tsd_received_count;
  
  if (m_hsi_passthrough == true){
    trigger_bitmask = MakeBitmask16(data.signal_map);
    LowHighBits = SplitBits(data.signal_map);   
    TLOG_DEBUG(3) << "Signal_map: " << data.signal_map << ", trigger bits: " << LowHighBits[1] << " " << LowHighBits[0];
    try {
      AnyBitSet(LowHighBits[1]);
    } catch (BadTriggerBitmask& e) {
      ers::error(e);
      return;
    }
  }

  triggeralgs::TriggerCandidate candidate;
  try {
    candidate = HSIEventToTriggerCandidate(data);
  } catch (SignalTypeError& e) {
    m_tc_sig_type_err_count++;
    ers::error(e);
    return;
  }

  bool successfullyWasSent = false;
  while (!successfullyWasSent) {
    try {
      m_output_queue->push(candidate, m_queue_timeout);
      successfullyWasSent = true;
      ++m_tc_sent_count;
    } catch (const dunedaq::appfwk::QueueTimeoutExpired& excpt) {
      std::ostringstream oss_warn;
      oss_warn << "push to output queue \"" << m_output_queue->get_name() << "\"";
      ers::warning(
        dunedaq::appfwk::QueueTimeoutExpired(ERS_HERE, get_name(), oss_warn.str(), m_queue_timeout.count()));
    }
  }
  m_tc_total_count++;
}

void
TimingTriggerCandidateMaker::do_scrap(const nlohmann::json&)
{}

void
TimingTriggerCandidateMaker::get_info(opmonlib::InfoCollector& ci, int /*level*/)
{
  timingtriggercandidatemakerinfo::Info i;

  i.tsd_received_count = m_tsd_received_count.load();
  i.tc_sent_count = m_tc_sent_count.load();
  i.tc_sig_type_err_count = m_tc_sig_type_err_count.load();
  i.tc_total_count = m_tc_total_count.load();

  ci.add(i);
}

} // namespace trigger
} // namespace dunedaq

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TimingTriggerCandidateMaker)
