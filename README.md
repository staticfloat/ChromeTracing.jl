# ChromeTracing.jl

[![CI](https://github.com/staticfloat/ChromeTracing.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/staticfloat/ChromeTracing.jl/actions/workflows/CI.yml)
[![codecov](https://codecov.io/gh/staticfloat/ChromeTracing.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/staticfloat/ChromeTracing.jl)
[![Docs](https://img.shields.io/badge/docs-stable-blue.svg)](https://staticfloat.github.io/ChromeTracing.jl)

Package for tracing Julia programs and writing those traces out as Chrome Trace Viewer JSON files.

## Quick start

```julia
using ChromeTracing

stream_trace("trace.json"; flush_interval=0.05)

@trace_event "startup" cat="app" args=Dict("msg" => "boot")

@trace_event "work" cat="compute" begin
    # your code
    sleep(0.01)
end

stop_streaming!()  # flushes and finalizes trace.json
```

Open `trace.json` in `chrome://tracing`.
You can also drag-and-drop it into perfetto at `https://ui.perfetto.dev`.

## Run packaged example

```bash
julia --threads=auto --project=. example.jl
```

