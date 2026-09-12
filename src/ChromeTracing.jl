"""
    ChromeTracing

Lightweight tracing for Julia programs, emitting Chrome trace event JSON files.
See: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU

Events are recorded with the [`@trace_event`](@ref) macro into a lock-free ring
buffer, and are then either dumped all at once with [`save_trace`](@ref) or
streamed to disk in the background with [`stream_trace`](@ref).

The resulting JSON file can be loaded into `chrome://tracing` or dragged into
the [perfetto web UI](https://ui.perfetto.dev).

# Example

```julia
using ChromeTracing

stream_trace("trace.json"; flush_interval=0.05)

@trace_event "startup" cat="app" args=Dict("msg" => "boot")

@trace_event "work" cat="compute" begin
    sleep(0.01)
end

stop_streaming!()  # flushes and finalizes trace.json
```
"""
module ChromeTracing

using JSON
using Base.Threads

export @trace_event
export save_trace, stream_trace, flush_trace!, stop_streaming!, clear_trace!

"""
    DEFAULT_BUFFER_LIMIT

Default maximum number of events buffered in memory at once.  Once the ring
buffer is full, newly recorded events are dropped and counted in
`StreamState.dropped` rather than blocking the calling thread.
"""
const DEFAULT_BUFFER_LIMIT = 100_000

"""
    DEFAULT_STREAM_FLUSH_INTERVAL

Default number of seconds the background writer started by
[`stream_trace`](@ref) sleeps between flushes of the buffered events.
"""
const DEFAULT_STREAM_FLUSH_INTERVAL = 0.1

"""
    StreamState

Internal state for the single global trace buffer and its (optional) background
writer task.

Events live in a fixed-size ring buffer (`slots`) that is written to without
locks: a producer claims a slot by atomically bumping `write_idx`, stores its
event, then publishes the slot by writing the claimed sequence number into
`slot_seq`.  The drain side only consumes a slot once its `slot_seq` matches the
sequence it expects, so a slot that has been claimed but not yet filled is never
read.
"""
mutable struct StreamState
    # Ring buffer of recorded events, `nothing` when empty.
    slots::Vector{Union{Nothing,Dict{String,Any}}}
    # Per-slot publish sequence. Writers set this after storing the event, so
    # the drain can tell whether a slot contains the current lap's event or is
    # still in-flight without taking a lock.
    slot_seq::Vector{Threads.Atomic{Int}}
    # Monotonically increasing count of claimed slots.
    write_idx::Threads.Atomic{Int}
    # Monotonically increasing count of drained slots.
    read_idx::Threads.Atomic{Int}
    # Set to `1` while a drain is in progress, so only one thread drains at a time.
    draining::Threads.Atomic{Int}
    # Number of slots in the ring buffer.
    capacity::Int
    # Number of events discarded because the buffer was full.
    dropped::Threads.Atomic{Int}
    # Background writer task, or `nothing` when not streaming.
    task::Union{Nothing,Task}
    # Flag used to ask the background writer to exit.
    stop::Base.RefValue{Bool}
    # Open trace file when streaming, or `nothing`.
    io::Union{Nothing,IOStream}
    # Whether at least one event has already been written to `io`, which
    # determines whether a separating comma is needed.
    file_has_events::Bool
end

const _stream_state = Ref{Union{Nothing,StreamState}}(nothing)

"""
    _make_stream_state(capacity::Int) -> StreamState

Allocate a fresh [`StreamState`](@ref) with a ring buffer of `capacity` slots
and no background writer attached.
"""
function _make_stream_state(capacity::Int)
    return StreamState(
        Union{Nothing,Dict{String,Any}}[nothing for _ in 1:capacity],
        [Threads.Atomic{Int}(0) for _ in 1:capacity],
        Threads.Atomic{Int}(0),
        Threads.Atomic{Int}(0),
        Threads.Atomic{Int}(0),
        capacity,
        Threads.Atomic{Int}(0),
        nothing,
        Ref(false),
        nothing,
        false,
    )
end

"""
    _ensure_stream_state() -> StreamState

Return the process-global [`StreamState`](@ref), lazily creating one with
[`DEFAULT_BUFFER_LIMIT`](@ref) slots if it does not exist yet.
"""
function _ensure_stream_state()
    state = _stream_state[]
    if state === nothing
        state = _make_stream_state(DEFAULT_BUFFER_LIMIT)
        _stream_state[] = state
    end
    return state
end

_ensure_stream_state()

"""
    _now_ts() -> Int

Current monotonic timestamp in microseconds, the unit for the `ts` field.
"""
_now_ts() = round(Int, time_ns() / 1000)

"""
    _normalize_args(args)

Convert `args` into a `Dict` with `String` keys, so that it round-trips through
JSON as an object.  `Dict`s, `NamedTuple`s, generators and tuples of pairs are
all accepted; anything else is returned unchanged.
"""
function _normalize_args(args)
    if args isa Dict
        return Dict(string(k) => v for (k, v) in args)
    elseif args isa NamedTuple
        return Dict(string(k) => v for (k, v) in pairs(args))
    elseif args isa Base.Generator
        return Dict(string(k) => v for (k, v) in args)
    elseif args isa Tuple
        return Dict(String(string(k)) => v for (k, v) in args)
    end
    return args
end

"""
    _coerce_trace_key(key) -> String

Convert a trace event key (a `Symbol`, `AbstractString`, or anything else) into
a `String` suitable for use as a JSON object key.
"""
function _coerce_trace_key(key)
    if key isa Symbol
        return String(key)
    elseif key isa AbstractString
        return String(key)
    else
        return string(key)
    end
end

"""
    build_event(name; kwargs...) -> Dict{String,Any}

Build a single Chrome trace event named `name`.

The standard fields are always present, defaulting as follows:

| Field | Default                            |
|:------|:-----------------------------------|
| `cat` | `""`                               |
| `ph`  | `"i"` (instant event)              |
| `ts`  | current timestamp, in microseconds |
| `pid` | `Base.getpid()`                    |
| `tid` | `Threads.threadid()`               |

The optional fields `dur`, `scope`, `cname`, `s`, `id`, `bp` and `metadata` are
copied through when supplied.  `args` is normalized into a `String`-keyed
`Dict`, and `extra` may be used to splat additional top-level keys into the
event.

# Example

```julia
ChromeTracing.build_event("work"; cat="compute", ph="X", dur=100)
```
"""
function build_event(name; kwargs...)
    event = Dict{String,Any}()
    event["name"] = string(name)

    event["cat"] = get(kwargs, :cat, "")
    event["ph"] = get(kwargs, :ph, "i")
    event["ts"] = get(kwargs, :ts, _now_ts())
    event["pid"] = get(kwargs, :pid, Base.getpid())
    event["tid"] = get(kwargs, :tid, Threads.threadid())

    for key in (:dur, :scope, :cname, :s, :id, :bp, :metadata)
        if haskey(kwargs, key)
            event[string(key)] = kwargs[key]
        end
    end

    if haskey(kwargs, :args)
        event["args"] = _normalize_args(kwargs[:args])
    end
    extra = Dict{String,Any}()
    if haskey(kwargs, :extra)
        for (k, v) in pairs(_normalize_args(kwargs[:extra]))
            extra[_coerce_trace_key(k)] = v
        end
    end
    for (k, v) in extra
        event[k] = v
    end

    return event
end

"""
    _write_json_array(path, events) -> Int

Write `events` to `path` as a single JSON array, creating the containing
directory if needed.  Returns the number of events written.
"""
function _write_json_array(path, events)
    mkpath(dirname(abspath(path)))
    open(path, write=true) do io
        JSON.print(io, events)
    end
    return length(events)
end

"""
    _reset_buffer!(stream::StreamState)

Discard every buffered event in `stream` and reset its ring buffer indices and
dropped-event counter back to their initial values.
"""
function _reset_buffer!(stream::StreamState)
    for i in eachindex(stream.slots)
        stream.slots[i] = nothing
        stream.slot_seq[i][] = 0
    end
    stream.write_idx[] = 0
    stream.read_idx[] = 0
    stream.draining[] = 0
    stream.dropped[] = 0
    return nothing
end

"""
    _drain_buffer!(stream::StreamState) -> Vector{Dict{String,Any}}

Remove and return every fully-published event from `stream`'s ring buffer, in
the order the events were recorded.

Only one thread drains at a time; if another drain is already in progress this
returns an empty vector immediately.  Draining also stops at the first slot that
has been claimed by a producer but not yet published, so events are never
returned out of order or half-written.
"""
function _drain_buffer!(stream::StreamState)
    # Don't allow multiple threads to drain at the same time
    if Threads.atomic_cas!(stream.draining, 0, 1) != 0
        return Dict{String,Any}[]
    end

    events = Dict{String,Any}[]

    try
        while true
            read_pos = stream.read_idx[]
            write_pos = stream.write_idx[]
            if read_pos >= write_pos
                break
            end

            slot = mod1(read_pos + 1, stream.capacity)
            expected_seq = read_pos + 1
            # If the sequence has not advanced yet, the writer has reserved the
            # slot but not finished publishing the event.
            if stream.slot_seq[slot][] != expected_seq
                break
            end

            event = stream.slots[slot]
            if event !== nothing
                push!(events, event)
            end
            stream.slots[slot] = nothing
            stream.slot_seq[slot][] = 0
            stream.read_idx[] = read_pos + 1
        end
    finally
        stream.draining[] = 0
    end

    return events
end

"""
    _append_event!(stream::StreamState, event) -> Bool

Append `event` to `stream`'s ring buffer without locking.

Returns `true` if the event was stored, or `false` if the buffer was full, in
which case the event is dropped and `stream.dropped` is incremented.  Dropping
rather than blocking keeps tracing overhead bounded on the hot path.
"""
function _append_event!(stream::StreamState, event)
    while true
        write_pos = stream.write_idx[]
        read_pos = stream.read_idx[]
        if (write_pos - read_pos) >= stream.capacity
            Threads.atomic_add!(stream.dropped, 1)
            return false
        end

        if Threads.atomic_cas!(stream.write_idx, write_pos, write_pos + 1) == write_pos
            slot = mod1(write_pos + 1, stream.capacity)
            stream.slots[slot] = event
            # Publish the slot only after the event is written.
            stream.slot_seq[slot][] = write_pos + 1
            return true
        end
    end
end

"""
    clear_trace!()

Stop any in-progress streaming (finalizing the trace file, see
[`stop_streaming!`](@ref)) and discard all buffered events.

Useful to get back to a known-empty state, e.g. between tests.
"""
function clear_trace!()
    stream = _stream_state[]
    if stream === nothing
        return nothing
    end
    stop_streaming!()
    _reset_buffer!(stream)
    return nothing
end

"""
    snapshot_default_buffer() -> Vector{Dict{String,Any}}

Drain and return every event currently buffered in the global trace buffer.

Note that this *removes* the events from the buffer; a second call immediately
afterwards returns only whatever was recorded in between.
"""
function snapshot_default_buffer()
    stream = _stream_state[]
    if stream === nothing
        return Dict{String,Any}[]
    end
    return _drain_buffer!(stream)
end

"""
    save_trace(path) -> Int

Drain every buffered event and write them to `path` as a Chrome trace JSON
array, returning the number of events written.

This is the one-shot counterpart to [`stream_trace`](@ref): record events first,
then dump them all at the end.

# Example

```julia
@trace_event "work" cat="compute" begin
    sleep(0.01)
end
save_trace("trace.json")
```
"""
function save_trace(path)
    p = String(path)
    events = snapshot_default_buffer()
    _write_json_array(p, events)
    return length(events)
end

"""
    _flush_stream_state(stream::StreamState; finalize::Bool=false) -> Int

Drain `stream`'s buffer and append the events to its open trace file, returning
the number of events written.  Returns `0` if `stream` is not currently writing
to a file.

When `finalize` is `true`, the closing `]` is written and the file is closed, so
the trace on disk is a complete JSON array.
"""
function _flush_stream_state(stream::StreamState; finalize::Bool=false)
    io = stream.io
    if io === nothing
        return 0
    end
    events = _drain_buffer!(stream)
    if !isempty(events)
        if stream.file_has_events
            write(io, ",\n")
        end
        for (idx, event) in enumerate(events)
            if idx > 1
                write(io, ",\n")
            end
            write(io, JSON.json(event))
        end
        stream.file_has_events = true
    end
    if finalize
        write(io, "\n]\n")
        close(io)
        stream.io = nothing
        stream.file_has_events = false
    end
    return length(events)
end

"""
    flush_trace!() -> Int

Immediately write any buffered events out to the file opened by
[`stream_trace`](@ref), without waiting for the background writer's next tick.
Returns the number of events written, or `0` if streaming is not active.

The trace file is left open and unterminated; use [`stop_streaming!`](@ref) to
finalize it.
"""
function flush_trace!()
    stream = _stream_state[]
    if stream === nothing || stream.io === nothing
        return 0
    end
    return _flush_stream_state(stream; finalize=false)
end

"""
    stop_streaming!()

Stop the background writer started by [`stream_trace`](@ref), flush any
remaining buffered events, and finalize the trace file so that it is a valid
JSON array.
"""
function stop_streaming!()
    stream = _stream_state[]
    if stream === nothing
        return nothing
    end
    stream.stop[] = true
    task = stream.task
    stream.task = nothing
    if task !== nothing
        wait(task)
    end
    _flush_stream_state(stream; finalize=true)
    return nothing
end

"""
    stream_trace(path; capacity=DEFAULT_BUFFER_LIMIT,
                 flush_interval=DEFAULT_STREAM_FLUSH_INTERVAL) -> StreamState

Open `path` for writing and start a background task that periodically flushes
recorded events to it, returning the [`StreamState`](@ref) being used.

`capacity` sets how many events may be buffered between flushes; once the buffer
is full, further events are dropped and counted in the returned state's
`dropped` field.  `flush_interval` is how long, in seconds, the writer sleeps
between flushes.

If a stream is already running it is stopped and finalized first.  Call
[`stop_streaming!`](@ref) when you are done so the trace file is closed properly.

# Example

```julia
stream = stream_trace("trace.json"; capacity=5000, flush_interval=0.05)
@trace_event "work" cat="compute" begin
    sleep(0.01)
end
stop_streaming!()
@info "dropped \$(stream.dropped[]) events"
```
"""
function stream_trace(path; capacity=DEFAULT_BUFFER_LIMIT, flush_interval=DEFAULT_STREAM_FLUSH_INTERVAL)
    p = String(path)
    stream = _ensure_stream_state()
    if stream.task !== nothing
        stop_streaming!()
    end
    stream.capacity = capacity
    mkpath(dirname(abspath(p)))
    stream.io = open(p, "w")
    write(stream.io, "[\n")
    stream.file_has_events = false
    stream.stop[] = false
    stream.task = @async begin
        while !stream.stop[]
            sleep(flush_interval)
            _flush_stream_state(stream)
        end
        _flush_stream_state(stream)
    end
    return stream
end

"""
    record_trace(name; kwargs...)

Build an event with [`build_event`](@ref) and append it to the global trace
buffer.  This is what [`@trace_event`](@ref) expands to; call it directly when
the event's name or keywords are only known at runtime.

The event is dropped silently if the buffer is full.
"""
function record_trace(name; kwargs...)
    event = build_event(name; kwargs...)

    stream = _ensure_stream_state()
    _append_event!(stream, event)
    return nothing
end

"""
    @trace_event name [key=value...]
    @trace_event name [key=value...] begin ... end

Record a Chrome trace event named `name`.

Keyword arguments are passed through to [`build_event`](@ref), so `cat`, `ph`,
`ts`, `pid`, `tid`, `dur`, `args` and friends may all be set.

In the first form a single event is emitted (an instant event, `ph="i"`, unless
you say otherwise).  In the second form the given block is wrapped in a matching
pair of `"B"`/`"E"` (begin/end) events, so the block shows up as a span in the
trace viewer.  The `"E"` event is emitted from a `finally` block, so spans are
closed even if the body throws.

# Examples

```julia
@trace_event "startup" cat="app" args=Dict("msg" => "boot")

@trace_event "work" cat="compute" begin
    sleep(0.01)
end
```
"""
macro trace_event(name, kws...)
    if !isempty(kws) && kws[end] isa Expr && kws[end].head === :block
        block = kws[end]
        block_args = kws[1:(end - 1)]
    else
        block = nothing
        block_args = kws
    end

    kw_exprs = []
    for kw in block_args
        if kw isa Expr && (kw.head === :kw || kw.head === :(=))
            key = kw.args[1]
            val = kw.args[2]
            if !(key isa Symbol)
                error("@trace_event expects keyword arguments like cat=\"perf\", ph=\"i\".")
            end
            push!(kw_exprs, Expr(:kw, key, esc(val)))
        else
            error("@trace_event expects keyword arguments like cat=\"perf\", ph=\"i\".")
        end
    end
        
    if block !== nothing
        return quote
            record_trace($(esc(name)); ph="B", $(kw_exprs...))
            try
                $(esc(block))
            finally
                record_trace($(esc(name)); ph="E", $(kw_exprs...))
            end
        end
    else
        if isempty(kw_exprs)
            return :(record_trace($(esc(name))))
        end
        return :(record_trace($(esc(name)); $(kw_exprs...)))
    end
end

end
