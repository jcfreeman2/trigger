/**
 * @file TokenManager_test.cxx  TokenManager class Unit Tests
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "appfwk/DAQSink.hpp"
#include "appfwk/DAQSource.hpp"

#include "networkmanager/NetworkManager.hpp"
#include "networkmanager/nwmgr/Structs.hpp"

#include "trigger/TokenManager.hpp"
#include "trigger/LivetimeCounter.hpp"

/**
 * @brief Name of this test module
 */
#define BOOST_TEST_MODULE TokenManager_test // NOLINT

#include "boost/test/unit_test.hpp"

#include <chrono>
#include <map>
#include <memory>
#include <string>

using namespace dunedaq;

BOOST_AUTO_TEST_SUITE(BOOST_TEST_MODULE)

/**
 * @brief Initializes the NetworkManager
 */
struct NetworkManagerTestFixture
{
  NetworkManagerTestFixture()
  {
    networkmanager::nwmgr::Connections testConfig;
    networkmanager::nwmgr::Connection testConn;
    testConn.name = "foo";
    testConn.address = "inproc://foo";
    testConn.topics = {};
    testConfig.push_back(testConn);
    networkmanager::NetworkManager::get().configure(testConfig);
  }
  ~NetworkManagerTestFixture() { networkmanager::NetworkManager::get().reset(); }

  NetworkManagerTestFixture(NetworkManagerTestFixture const&) = default;
  NetworkManagerTestFixture(NetworkManagerTestFixture&&) = default;
  NetworkManagerTestFixture& operator=(NetworkManagerTestFixture const&) = default;
  NetworkManagerTestFixture& operator=(NetworkManagerTestFixture&&) = default;
};

BOOST_TEST_GLOBAL_FIXTURE(NetworkManagerTestFixture);

BOOST_AUTO_TEST_CASE(Basics)
{
  using namespace std::chrono_literals;

  int initial_tokens = 10;
  daqdataformats::run_number_t run_number = 1;
  auto livetime_counter=std::make_shared<trigger::LivetimeCounter>(trigger::LivetimeCounter::State::kPaused);
  trigger::TokenManager tm("foo", initial_tokens, run_number, livetime_counter);

  BOOST_CHECK_EQUAL(tm.get_n_tokens(), initial_tokens);
  BOOST_CHECK_EQUAL(tm.triggers_allowed(), true);

  for (int i = 0; i < initial_tokens - 1; ++i) {
    tm.trigger_sent(i);
    BOOST_CHECK_EQUAL(tm.get_n_tokens(), initial_tokens - i - 1);
    BOOST_CHECK_EQUAL(tm.triggers_allowed(), true);
  }

  tm.trigger_sent(initial_tokens);
  BOOST_CHECK_EQUAL(tm.get_n_tokens(), 0);
  BOOST_CHECK_EQUAL(tm.triggers_allowed(), false);

  // Send a token and check that triggers become allowed again
  dfmessages::TriggerDecisionToken token;
  token.run_number = run_number;
  token.trigger_number = 1;
  auto serialised_token = dunedaq::serialization::serialize(token, dunedaq::serialization::kMsgPack);
  networkmanager::NetworkManager::get().send_to(
    "foo", static_cast<const void*>(serialised_token.data()), serialised_token.size(), std::chrono::milliseconds(10));

  // Give TokenManager a little time to pop the token off the queue
  std::this_thread::sleep_for(100ms);
  BOOST_CHECK_EQUAL(tm.get_n_tokens(), 1);
  BOOST_CHECK_EQUAL(tm.triggers_allowed(), true);
}

BOOST_AUTO_TEST_SUITE_END()
