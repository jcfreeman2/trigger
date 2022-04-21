/**
 * @file streamed_TPs_to_text.cxx Dump streamed TPs from an HDF5 into the text format used by trigger
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */
#include "CLI/CLI.hpp"

#include "detdataformats/trigger/TriggerPrimitive.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"

#include <daqdataformats/TimeSlice.hpp>
#include <daqdataformats/TimeSliceHeader.hpp>
#include <daqdataformats/Types.hpp>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>

int
main(int argc, char** argv)
{
  CLI::App app{ "Dump streamed TPs from an HDF5 into the text format used by trigger" };

  std::string in_filename;
  app.add_option("-i,--input", in_filename, "Input HDF5 file");

  std::string out_filename;
  app.add_option("-o,--output", out_filename, "Output text file");

  CLI11_PARSE(app, argc, argv);

  dunedaq::hdf5libs::HDF5RawDataFile hdf5file(in_filename);

  std::ofstream fout(out_filename);
  
  auto fragment_paths = hdf5file.get_all_fragment_dataset_paths();

  using dunedaq::detdataformats::trigger::TriggerPrimitive;
  
  // Populate the map with the TRHs and DS fragments
  for (auto fragment_path: fragment_paths){
    auto frag = hdf5file.get_frag_ptr(fragment_path);
    size_t payload_size = frag->get_size() - sizeof(dunedaq::daqdataformats::FragmentHeader);
    size_t n_tps = payload_size / sizeof(TriggerPrimitive);
    size_t remainder = payload_size % sizeof(TriggerPrimitive);
    assert(remainder == 0);
    const TriggerPrimitive* prim = reinterpret_cast<TriggerPrimitive*>(frag->get_data());
    for (size_t i = 0; i < n_tps; ++i) {
      fout << "\t" << prim->time_start
                << "\t" << prim->time_over_threshold
                << "\t" << prim->time_peak
                << "\t" << prim->channel
                << "\t" << prim->adc_integral
                << "\t" << prim->adc_peak
                << "\t" << prim->detid
                << "\t" << prim->type << std::endl;
      ++prim;
    }

  }
}
