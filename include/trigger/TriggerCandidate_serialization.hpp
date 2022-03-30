/**
 * @file TriggerCandidate_serialization.hpp
 *
 * This file contains the macro calls needed to allow
 * TriggerCandidates from triggeralgs to be serialized using the
 * dunedaq serialization package
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef TRIGGER_INCLUDE_TRIGGER_TRIGGERCANDIDATE_SERIALIZATION_HPP_
#define TRIGGER_INCLUDE_TRIGGER_TRIGGERCANDIDATE_SERIALIZATION_HPP_

#include "serialization/Serialization.hpp"
#include "trigger/TriggerActivity_serialization.hpp"
#include "triggeralgs/TriggerCandidate.hpp"

MSGPACK_ADD_ENUM(triggeralgs::TriggerCandidate::Type)

MSGPACK_ADD_ENUM(triggeralgs::TriggerCandidate::Algorithm)

                                 
DUNE_DAQ_SERIALIZE_NON_INTRUSIVE(triggeralgs,
                                 TriggerCandidate,
                                 time_start,
                                 time_end,
                                 time_candidate,
                                 detid,
                                 type,
                                 algorithm,
                                 version,
                                 inputs)

#endif // TRIGGER_INCLUDE_TRIGGER_TRIGGERCANDIDATE_SERIALIZATION_HPP_
