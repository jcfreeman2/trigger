local moo = import "moo.jsonnet";

// A schema builder in the given path (namespace)
local ns = "dunedaq.trigger.tpbufferconfig";
local s = moo.oschema.schema(ns);

local s_readoutconfig = import "readoutlibs/readoutconfig.jsonnet";
local readoutconfig = moo.oschema.hier(s_readoutconfig).dunedaq.readoutlibs.readoutconfig;

// Object structure used by the test/fake producer module
local tpbufferconfig = {
      conf: s.record("Conf", [
        s.field("latencybufferconf", readoutconfig.LatencyBufferConf, doc="Latency Buffer config"),
        s.field("requesthandlerconf", readoutconfig.RequestHandlerConf, doc="Request Handler config"),

      ], doc="TPBuffer configuration"),

};

s_readoutconfig + moo.oschema.sort_select(tpbufferconfig, ns)
