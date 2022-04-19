// This is the application info schema used by the module level trigger module.
// It describes the information object structure passed by the application 
// for operational monitoring

local moo = import "moo.jsonnet";
local s = moo.oschema.schema("dunedaq.trigger.moduleleveltriggerinfo");

local info = {
    uint8  : s.number("uint8", "u8",
                     doc="An unsigned of 8 bytes"),

   info: s.record("Info", [
       s.field("tc_received_count",                  self.uint8, 0, doc="Number of trigger candidates received."), 
       s.field("td_sent_count",                      self.uint8, 0, doc="Number of trigger decisions added to queue."), 
       s.field("td_queue_timeout_expired_err_count", self.uint8, 0, doc="Number of trigger decisions failed to be added to queue due to timeout."),
       s.field("td_inhibited_count",                 self.uint8, 0, doc="Number of trigger decisions inhibited."), 
       s.field("td_paused_count",                    self.uint8, 0, doc="Number of trigger decisions created during pause mode."), 
       s.field("td_total_count",                     self.uint8, 0, doc="Total number of trigger decisions created."),
       s.field("lc_total_deadtime",		     self.uint8, 0, doc="Total deadtime accumulated this run.") 
   ], doc="Module level trigger information")
};

moo.oschema.sort_select(info) 
