/**
 * @file TCTee.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2022.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */


#include "trigger/Tee.hpp"
#include "triggeralgs/TriggerCandidate.hpp"

namespace dunedaq::trigger {

using TCTee = Tee<triggeralgs::TriggerCandidate>;

} // namespace dunedaq::trigger

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TCTee)
