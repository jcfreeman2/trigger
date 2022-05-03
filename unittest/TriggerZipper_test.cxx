/**
 * @file TriggerZipper_test.cxx unit tests related to zipper
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

// use TP as representative specialization
#include "../plugins/TPZipper.hpp" // NOLINT

#include "iomanager/IOManager.hpp"

/**
 * @brief Name of this test module
 */
#define BOOST_TEST_MODULE TriggerZipper_test // NOLINT
#include "boost/test/unit_test.hpp"

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>

using namespace dunedaq;

BOOST_AUTO_TEST_SUITE(BOOST_TEST_MODULE)

BOOST_AUTO_TEST_CASE(TPSet_GeoID_Init_System_Type_Is_DataSelection)
{
  trigger::TPSet tpset;
  BOOST_CHECK_EQUAL(tpset.origin.system_type, daqdataformats::GeoID::SystemType::kDataSelection);
}

BOOST_AUTO_TEST_CASE(ZipperStreamIDFromGeoID)
{
  trigger::TPSet tpset1, tpset2;

  tpset1.origin.region_id = 1;
  tpset1.origin.element_id = 1;
  tpset2.origin.region_id = 2;
  tpset2.origin.element_id = 2;

  auto id1 = trigger::zipper_stream_id(tpset1.origin);
  auto id2 = trigger::zipper_stream_id(tpset2.origin);

  // With C++23, we may change from LL to Z :)
  size_t base = 3LL << 48;
  size_t n1 = base | (1LL << 32) | 1LL;
  size_t n2 = base | (2LL << 32) | 2LL;

  BOOST_CHECK_EQUAL(n1, id1);
  BOOST_CHECK_EQUAL(n2, id2);
}

using tpset_queue_t = appfwk::Queue<trigger::TPSet>;
using duration_t = tpset_queue_t::duration_t;
using tpset_queue_ptr = std::shared_ptr<tpset_queue_t>;

struct TPSetSrc
{
  uint32_t element_id{ 0 }; // NOLINT(build/unsigned)
  using timestamp_t = daqdataformats::timestamp_t;
  timestamp_t dt{ 10 };

  trigger::TPSet tpset{};

  trigger::TPSet operator()(timestamp_t datatime)
  {
    ++tpset.seqno;
    tpset.origin.region_id = 0;
    tpset.origin.element_id = element_id;
    tpset.start_time = datatime;
    tpset.end_time = datatime + dt;
    return tpset;
  }
};

static void
pop_must_timeout(tpset_queue_ptr out)
{
  TLOG() << "Popping assuming a timeout";
  trigger::TPSet tpset;
  BOOST_CHECK_THROW(out->pop(tpset, (duration_t)1000), appfwk::QueueTimeoutExpired);
}
static trigger::TPSet
pop_must_succeed(tpset_queue_ptr out)
{
  TLOG() << "Popping assuming no waiting";
  trigger::TPSet tpset;
  BOOST_CHECK_NO_THROW(out->pop(tpset, (duration_t)1000); // no exception expected
  );
  TLOG() << "Popped " << tpset.origin << " @ " << tpset.start_time ;
  return tpset;
}

static void
push0(tpset_queue_ptr in, trigger::TPSet&& tpset)
{
  TLOG() << "Pushing " << tpset.origin << " @ " << tpset.start_time ;
  assert(in->can_push());
  in->push(std::move(tpset), (duration_t)0);
}

BOOST_AUTO_TEST_CASE(ZipperScenario1)
{
  auto& qr = appfwk::QueueRegistry::get();

  qr.configure({ { "source", appfwk::QueueConfig{ appfwk::QueueConfig::kStdDeQueue, 10 } },
                 { "sink", appfwk::QueueConfig{ appfwk::QueueConfig::kStdDeQueue, 10 } } });

  auto in = qr.get_queue<trigger::TPSet>("source");
  auto out = qr.get_queue<trigger::TPSet>("sink");

  auto zip = std::make_unique<trigger::TPZipper>("zs1");

  zip->set_input("source");
  zip->set_output("sink");

  trigger::TPZipper::cfg_t cfg{ 2, 100, 1, 20 };
  nlohmann::json jcfg = cfg, jempty;
  zip->do_configure(jcfg);

  TPSetSrc s1{ 1 }, s2{ 2 };

  zip->do_start(jempty);

  push0(in, s1(10));
  push0(in, s2(12));

  pop_must_timeout(out);

  push0(in, s1(11));
  push0(in, s2(13));

  auto got = pop_must_succeed(out);
  BOOST_CHECK_EQUAL(got.start_time, 10);

  push0(in, s1(14));

  got = pop_must_succeed(out);
  BOOST_CHECK_EQUAL(got.start_time, 11);

  zip->do_stop(jempty); // triggers a flush

  got = pop_must_succeed(out);
  BOOST_CHECK_EQUAL(got.start_time, 12);
  got = pop_must_succeed(out);
  BOOST_CHECK_EQUAL(got.start_time, 13);
  got = pop_must_succeed(out);
  BOOST_CHECK_EQUAL(got.start_time, 14);

  TLOG() << "Deleteing TriggerZipper";
  zip.reset(nullptr);
}

BOOST_AUTO_TEST_SUITE_END()
