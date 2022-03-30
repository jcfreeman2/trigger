/**
 * @file TriggerActivity_serialization.hpp
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_INCLUDE_TRIGGER_TRIGGERACTIVITY_SERIALIZATION_HPP_
#define TRIGGER_INCLUDE_TRIGGER_TRIGGERACTIVITY_SERIALIZATION_HPP_

#include "serialization/Serialization.hpp"
#include "trigger/TriggerPrimitive_serialization.hpp"
#include "triggeralgs/TriggerActivity.hpp"

MSGPACK_ADD_ENUM(triggeralgs::TriggerActivity::Type)

MSGPACK_ADD_ENUM(triggeralgs::TriggerActivity::Algorithm)

DUNE_DAQ_SERIALIZE_NON_INTRUSIVE(dunedaq::detdataformats::trigger,
                                 TriggerActivityData,
                                 time_start,
                                 time_end,
                                 time_peak,
                                 time_activity,
                                 channel_start,
                                 channel_end,
                                 channel_peak,
                                 adc_integral,
                                 adc_peak,
                                 detid,
                                 type,
                                 algorithm,
                                 version)

DUNE_DAQ_SERIALIZE_NON_INTRUSIVE(triggeralgs,
                                 TriggerActivity,
                                 time_start,
                                 time_end,
                                 time_peak,
                                 time_activity,
                                 channel_start,
                                 channel_end,
                                 channel_peak,
                                 adc_integral,
                                 adc_peak,
                                 detid,
                                 type,
                                 algorithm,
                                 version,
                                 inputs)

#endif // TRIGGER_INCLUDE_TRIGGER_TRIGGERACTIVITY_SERIALIZATION_HPP_
