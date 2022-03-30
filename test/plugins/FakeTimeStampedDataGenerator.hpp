/**
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_TEST_PLUGINS_FAKETIMESTAMPEDDATAGENERATOR_HPP_
#define TRIGGER_TEST_PLUGINS_FAKETIMESTAMPEDDATAGENERATOR_HPP_

#include "dfmessages/HSIEvent.hpp"

#include "trigger/faketimestampeddatagenerator/Structs.hpp"

#include "appfwk/DAQModule.hpp"
#include "appfwk/DAQSink.hpp"
#include "utilities/WorkerThread.hpp"

#include <ers/Issue.hpp>

#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace dunedaq {
namespace trigger {

/**
 * @brief RandomDataListGenerator creates vectors of ints and writes
 * them to the configured output queues.
 */
class FakeTimeStampedDataGenerator : public dunedaq::appfwk::DAQModule
{
public:
  /**
   * @brief FakeTimeStampedDataGenerator Constructor
   * @param name Instance name for this FakeTimeStampedDataGenerator instance
   */
  explicit FakeTimeStampedDataGenerator(const std::string& name);

  FakeTimeStampedDataGenerator(const FakeTimeStampedDataGenerator&) =
    delete; ///< FakeTimeStampedDataGenerator is not copy-constructible
  FakeTimeStampedDataGenerator& operator=(const FakeTimeStampedDataGenerator&) =
    delete; ///< FakeTimeStampedDataGenerator is not copy-assignable
  FakeTimeStampedDataGenerator(FakeTimeStampedDataGenerator&&) =
    delete; ///< FakeTimeStampedDataGenerator is not move-constructible
  FakeTimeStampedDataGenerator& operator=(FakeTimeStampedDataGenerator&&) =
    delete; ///< FakeTimeStampedDataGenerator is not move-assignable

  void init(const nlohmann::json& obj) override;
  void get_info(opmonlib::InfoCollector& ci, int level) override;

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
  using sink_t = dunedaq::appfwk::DAQSink<dfmessages::HSIEvent>;
  std::unique_ptr<sink_t> m_outputQueue;
  std::chrono::milliseconds m_queueTimeout;

  // Random Generatior
  dfmessages::HSIEvent get_hsievent();
  std::default_random_engine m_generator;
  std::uniform_int_distribution<int> m_rdm_signaltype = std::uniform_int_distribution<int>(0, 2);
  uint32_t m_counts; // NOLINT(build/unsigned)

  uint64_t m_sleep_time; // NOLINT(build/unsigned)
  uint64_t m_frequency;  // NOLINT(build/unsigned)
};
} // namespace trigger
} // namespace dunedaq

#endif // TRIGGER_TEST_PLUGINS_FAKETIMESTAMPEDDATAGENERATOR_HPP_

// Local Variables:
// c-basic-offset: 2
// End:
