local moo = import "moo.jsonnet";
local ns = "dunedaq.trigger.tasetsink";
local s = moo.oschema.schema(ns);

local hier = {
    filename: s.string("Filename"),
    bool: s.boolean("Bool"),
  
    conf : s.record("Conf", [
        s.field("output_filename", hier.filename,
          doc="Output filename"),
      s.field("do_checks", hier.bool, default=true,
        doc="Whether to do sanity checks on the input TASets"),
    ], doc="TASetSink configuration"),

  
};

moo.oschema.sort_select(hier, ns)
