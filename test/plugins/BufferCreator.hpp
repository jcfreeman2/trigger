/**
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_TEST_PLUGINS_BUFFERCREATOR_HPP_
#define TRIGGER_TEST_PLUGINS_BUFFERCREATOR_HPP_

#include "dfmessages/HSIEvent.hpp"

#include "trigger/buffercreator/Structs.hpp"

#include "appfwk/DAQModule.hpp"
#include "appfwk/DAQSink.hpp"
#include "appfwk/ThreadHelper.hpp"

#include <ers/Issue.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace dunedaq {
namespace trigger {

/**
 * @brief BufferCreator creates a buffer that stores TPSets and handles data requests.
 */
class BufferCreator : public dunedaq::appfwk::DAQModule
{
public:
  /**
   * @brief BufferCreator Constructor
   * @param name Instance name for this BufferCreator instance
   */
  explicit BufferCreator(const std::string& name);

  BufferCreator(const BufferCreator&) =
    delete; ///< BufferCreator is not copy-constructible
  BufferCreator& operator=(const BufferCreator&) =
    delete; ///< BufferCreator is not copy-assignable
  BufferCreator(BufferCreator&&) =
    delete; ///< BufferCreator is not move-constructible
  BufferCreator& operator=(BufferCreator&&) =
    delete; ///< BufferCreator is not move-assignable

  void init(const nlohmann::json& obj) override;
  void get_info(opmonlib::InfoCollector& ci, int level) override;

private:
  // Commands
  void do_configure(const nlohmann::json& obj);
  void do_start(const nlohmann::json& obj);
  void do_stop(const nlohmann::json& obj);
  void do_scrap(const nlohmann::json& obj);

  // Threading
  dunedaq::appfwk::ThreadHelper m_thread;
  void do_work(std::atomic<bool>&);

  // Configuration
  using sink_t = dunedaq::appfwk::DAQSink<dfmessages::HSIEvent>;
  std::unique_ptr<sink_t> m_outputQueue;
  std::chrono::milliseconds m_queueTimeout;

  uint64_t m_buffer_size;
  uint64_t m_sleep_time;
};
} // namespace trigger
} // namespace dunedaq

#endif // TRIGGER_TEST_PLUGINS_BUFFERCREATOR_HPP_

// Local Variables:
// c-basic-offset: 2
// End:
