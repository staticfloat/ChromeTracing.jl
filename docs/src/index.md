```@meta
CurrentModule = ChromeTracing
```

# ChromeTracing.jl

```@docs
ChromeTracing
```

## Recording events

Events are recorded with the [`@tracepoint`](@ref) macro. In its single-argument
form it emits an *instant* event; given a trailing `begin ... end` block it wraps
the block in a matching pair of begin/end events, which the trace viewer renders
as a span:

```julia
using ChromeTracing

@tracepoint "startup" cat="app" args=Dict("msg" => "boot")

@tracepoint "work" cat="compute" begin
    sleep(0.01)
end
```

Any keyword accepted by [`build_event`](@ref) may be passed to `@tracepoint`,
including `cat`, `ph`, `ts`, `pid`, `tid`, `dur` and `args`.

### One-shot Saving: `save_trace`

Record everything into the in-memory buffer, then dump it at the end:

```julia
@tracepoint "work" cat="compute" begin
    sleep(0.01)
end

save_trace("trace.json")
```

### Streaming: `stream_trace`

For long-running programs, start a background writer that periodically flushes
buffered events to disk. Call [`stop_streaming!`](@ref) when you are done so the
file is finalized into a valid JSON array:

```julia
stream_trace("trace.json"; capacity=5000, flush_interval=0.05)

@tracepoint "work" cat="compute" begin
    sleep(0.01)
end

stop_streaming!()
```

[`flush_trace!`](@ref) forces a flush without waiting for the next tick, and
[`clear_trace!`](@ref) discards everything buffered so far.

## Dropped events

The event buffer is a fixed-size, lock-free ring buffer. When producers outrun
the writer and the buffer fills up, new events are **dropped** rather than
blocking the calling thread, which keeps tracing overhead bounded. The number of
dropped events is available on the state returned by [`stream_trace`](@ref):

```julia
stream = stream_trace("trace.json")
# ... work ...
stop_streaming!()
@info "dropped $(stream.dropped[]) events"
```

Raise `capacity` or lower `flush_interval` if you are dropping more than you can
afford.

## Viewing the trace

Open the resulting JSON file in `chrome://tracing`, or drag and drop it into
[perfetto](https://ui.perfetto.dev).

## Running the packaged example

```bash
julia --threads=auto --project=. example.jl
```
