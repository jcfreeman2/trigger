/**
 * @file TASetTee.cpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2022.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */


#include "trigger/Tee.hpp"
#include "trigger/TASet.hpp"

namespace dunedaq::trigger {

using TASetTee = Tee<TASet>;

} // namespace dunedaq::trigger

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TASetTee)
