/**
 * @file TPSetTee.hpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2022.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "trigger/Tee.hpp"
#include "trigger/TPSet.hpp"

namespace dunedaq::trigger {

using TPSetTee = Tee<TPSet>;

} // namespace dunedaq::trigger 

DEFINE_DUNE_DAQ_MODULE(dunedaq::trigger::TPSetTee)
