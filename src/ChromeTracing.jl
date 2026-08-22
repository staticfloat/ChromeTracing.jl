module ChromeTracing

using JSON
using Base.Threads

export @tracepoint
export save_trace, stream_trace, flush_trace!, stop_streaming!, clear_trace!

# Default maximum number of events we'll buffer in memory at once
const DEFAULT_BUFFER_LIMIT = 100_000

# How often the background writer flushes buffered events.
const DEFAULT_STREAM_FLUSH_INTERVAL = 0.1

mutable struct StreamState
    slots::Vector{Union{Nothing,Dict{String,Any}}}
    # Per-slot publish sequence. Writers set this after storing the event, so
    # the drain can tell whether a slot contains the current lap's event or is
    # still in-flight without taking a lock.
    slot_seq::Vector{Threads.Atomic{Int}}
    write_idx::Threads.Atomic{Int}
    read_idx::Threads.Atomic{Int}
    draining::Threads.Atomic{Int}
    capacity::Int
    dropped::Threads.Atomic{Int}
    task::Union{Nothing,Task}
    stop::Base.RefValue{Bool}
    io::Union{Nothing,IOStream}
    file_has_events::Bool
end

const _stream_state = Ref{Union{Nothing,StreamState}}(nothing)

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

function _ensure_stream_state()
    state = _stream_state[]
    if state === nothing
        state = _make_stream_state(DEFAULT_BUFFER_LIMIT)
        _stream_state[] = state
    end
    return state
end

_ensure_stream_state()

_now_ts() = round(Int, time_ns() / 1000)

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

function _coerce_trace_key(key)
    if key isa Symbol
        return String(key)
    elseif key isa AbstractString
        return String(key)
    else
        return string(key)
    end
end

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

function _write_json_array(path, events)
    mkpath(dirname(abspath(path)))
    open(path, write=true) do io
        JSON.print(io, events)
    end
    return length(events)
end

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

function clear_trace!()
    stream = _stream_state[]
    if stream === nothing
        return nothing
    end
    stop_streaming!()
    _reset_buffer!(stream)
    return nothing
end

function snapshot_default_buffer()
    stream = _stream_state[]
    if stream === nothing
        return Dict{String,Any}[]
    end
    return _drain_buffer!(stream)
end

function save_trace(path)
    p = String(path)
    events = snapshot_default_buffer()
    _write_json_array(p, events)
    return length(events)
end

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

function flush_trace!()
    stream = _stream_state[]
    if stream === nothing || stream.io === nothing
        return 0
    end
    return _flush_stream_state(stream; finalize=false)
end

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

function record_trace(name; kwargs...)
    event = build_event(name; kwargs...)

    stream = _ensure_stream_state()
    _append_event!(stream, event)
    return nothing
end

macro tracepoint(name, kws...)
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
                error("@tracepoint expects keyword arguments like cat=\"perf\", ph=\"i\".")
            end
            push!(kw_exprs, Expr(:kw, key, esc(val)))
        else
            error("@tracepoint expects keyword arguments like cat=\"perf\", ph=\"i\".")
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
