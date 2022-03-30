/**
 * @file TriggerPrimitive_serialization.hpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_INCLUDE_TRIGGER_TRIGGERPRIMITIVE_SERIALIZATION_HPP_
#define TRIGGER_INCLUDE_TRIGGER_TRIGGERPRIMITIVE_SERIALIZATION_HPP_

#include "serialization/Serialization.hpp"
#include "triggeralgs/TriggerPrimitive.hpp"
#include "detdataformats/trigger/Types.hpp"

MSGPACK_ADD_ENUM(dunedaq::detdataformats::trigger::TriggerPrimitive::Type)

MSGPACK_ADD_ENUM(dunedaq::detdataformats::trigger::TriggerPrimitive::Algorithm)

DUNE_DAQ_SERIALIZE_NON_INTRUSIVE(dunedaq::detdataformats::trigger,
                                 TriggerPrimitive,
                                 time_start,
                                 time_peak,
                                 time_over_threshold,
                                 channel,
                                 adc_integral,
                                 adc_peak,
                                 detid,
                                 type,
                                 algorithm,
                                 version,
                                 flag)

#endif // TRIGGER_INCLUDE_TRIGGER_TRIGGERPRIMITIVE_SERIALIZATION_HPP_
