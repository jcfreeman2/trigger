local moo = import "moo.jsonnet";
local ns = "dunedaq.trigger.timingtriggercandidatemaker";
local s = moo.oschema.schema(ns);

local types = {
	time_t : s.number("time_t", "i8", doc="Time"),
	signal_type_t : s.number("signal_type_t", "u4", doc="Signal type"),
	connection_name : s.string("connection_name"),
	hsi_tt_pt : s.boolean("hsi_tt_pt"),
	map_t : s.record("map_t", [
			s.field("signal_type",
				self.signal_type_t,
				0,
				doc="Readout time before time stamp"),
			s.field("time_before",
				self.time_t,
				10000,
				doc="Readout time before time stamp"),
			s.field("time_after",
				self.time_t,
				20000,
				doc="Readout time after time stamp"),
			], doc="Map from signal type to readout window"),
	conf: s.record("Conf", [
		s.field("s0",
			self.map_t,
			{
				signal_type: 0,
				time_before: 10000,
				time_after: 20000
			} ,
			doc="Iceberg"),
		s.field("s1",
			self.map_t,
			{
				signal_type: 1,
				time_before: 100000,
				time_after: 200000
			},
			doc="Example 1"),
		s.field("s2",
			self.map_t,
			{
				signal_type: 2,
				time_before: 1000000,
				time_after: 2000000
			},
			doc="Example 2"),
		s.field("hsievent_connection_name", 
			self.connection_name, 
			doc="Connection name to be used to send hsievent to"),
		s.field("hsi_trigger_type_passthrough", self.hsi_tt_pt, doc="Option to override the trigger type values")
	], doc="Configuration of the different readout time maps"),

};

moo.oschema.sort_select(types, ns)
