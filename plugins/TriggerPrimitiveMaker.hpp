/**
 * @file TriggerPrimitiveMaker.hpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_PLUGINS_TRIGGERPRIMITIVEMAKER_HPP_
#define TRIGGER_PLUGINS_TRIGGERPRIMITIVEMAKER_HPP_

#include "triggeralgs/TriggerPrimitive.hpp"

#include "appfwk/DAQModule.hpp"
#include "appfwk/DAQSink.hpp"
#include "appfwk/DAQSource.hpp"
#include "utilities/WorkerThread.hpp"

#include "trigger/TPSet.hpp"
#include "trigger/triggerprimitivemaker/Nljs.hpp"

#include <memory>
#include <string>
#include <vector>

namespace dunedaq {
namespace trigger {
class TriggerPrimitiveMaker : public dunedaq::appfwk::DAQModule
{
public:
  /**
   * @brief RandomDataListGenerator Constructor
   * @param name Instance name for this RandomDataListGenerator instance
   */
  explicit TriggerPrimitiveMaker(const std::string& name);

  TriggerPrimitiveMaker(const TriggerPrimitiveMaker&) = delete; ///< TriggerPrimitiveMaker is not copy-constructible
  TriggerPrimitiveMaker& operator=(const TriggerPrimitiveMaker&) =
    delete;                                                ///< TriggerPrimitiveMaker is not copy-assignable
  TriggerPrimitiveMaker(TriggerPrimitiveMaker&&) = delete; ///< TriggerPrimitiveMaker is not move-constructible
  TriggerPrimitiveMaker& operator=(TriggerPrimitiveMaker&&) = delete; ///< TriggerPrimitiveMaker is not move-assignable

  void init(const nlohmann::json& obj) override;

private:
  // Commands
  void do_configure(const nlohmann::json& obj);
  void do_start(const nlohmann::json& obj);
  void do_stop(const nlohmann::json& obj);
  void do_scrap(const nlohmann::json& obj);

  // Threading
  dunedaq::utilities::WorkerThread m_thread;
  void do_work(std::atomic<bool>&);

  // Configuration
  triggerprimitivemaker::ConfParams m_conf;
  std::unique_ptr<appfwk::DAQSink<TPSet>> m_tpset_sink;
  std::vector<TPSet> m_tpsets;

  std::chrono::milliseconds m_queue_timeout;
};
} // namespace trigger
} // namespace dunedaq

#endif // TRIGGER_PLUGINS_TRIGGERPRIMITIVEMAKER_HPP_
