/**
 * @file TPSetReceiver.hpp
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_PLUGINS_TPSETRECEIVER_HPP_
#define TRIGGER_PLUGINS_TPSETRECEIVER_HPP_

#include "trigger/TPSet.hpp"

#include "appfwk/DAQModule.hpp"
#include "iomanager/Receiver.hpp"
#include "iomanager/Sender.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace dunedaq {
namespace trigger {

/**
 * @brief TPSetReceiver receives requests then dispatches them to the appropriate queue
 */
class TPSetReceiver : public dunedaq::appfwk::DAQModule
{
public:
  /**
   * @brief TPSetReceiver Constructor
   * @param name Instance name for this TPSetReceiver instance
   */
  explicit TPSetReceiver(const std::string& name);

  TPSetReceiver(const TPSetReceiver&) = delete;            ///< TPSetReceiver is not copy-constructible
  TPSetReceiver& operator=(const TPSetReceiver&) = delete; ///< TPSetReceiver is not copy-assignable
  TPSetReceiver(TPSetReceiver&&) = delete;                 ///< TPSetReceiver is not move-constructible
  TPSetReceiver& operator=(TPSetReceiver&&) = delete;      ///< TPSetReceiver is not move-assignable

  void init(const data_t&) override;

private:
  // Commands
  void do_conf(const data_t&);
  void do_start(const data_t&);
  void do_stop(const data_t&);
  void do_scrap(const data_t&);

  void get_info(opmonlib::InfoCollector& ci, int level) override;

  void dispatch_tpset(trigger::TPSet);

  // Configuration
  std::chrono::milliseconds m_queue_timeout;
  std::string m_topic;

  // Queue(s)
  using tpsetsink_t = dunedaq::iomanager::SenderConcept<trigger::TPSet>;
  std::map<daqdataformats::GeoID, std::unique_ptr<tpsetsink_t>> m_tpset_output_queues;

  std::atomic<uint64_t> m_received_tpsets{ 0 }; // NOLINT (build/unsigned)
};
} // namespace trigger
} // namespace dunedaq

#endif // TRIGGER_PLUGINS_TPSETRECEIVER_HPP_
