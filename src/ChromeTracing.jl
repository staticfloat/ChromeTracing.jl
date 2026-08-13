module ChromeTracing

using JSON
using Base.Threads

export @tracepoint
export save_trace, stream_trace, flush_trace!, stop_streaming!, clear_trace!, total_dropped

# Default maximum number of events we'll buffer in memory at once
const DEFAULT_BUFFER_LIMIT = 100_000

# How often we'll 
const DEFAULT_STREAM_FLUSH_INTERVAL = 0.1

mutable struct ThreadBuffer
    buffers::Vector{Vector{Dict{String,Any}}}
    locks::Vector{ReentrantLock}
    active_idx::Threads.Atomic{Int}
    max_buffer::Threads.Atomic{Int}
    dropped::Threads.Atomic{Int}
end

mutable struct StreamState
    path::Union{Nothing,String}
    buffers::Vector{ThreadBuffer}
    task::Union{Nothing,Task}
    stop::Base.RefValue{Bool}
end

const _stream_state = Ref{Union{Nothing,StreamState}}(nothing)

function _make_thread_buffers(n::Int, max_buffer::Int)
    return [
        ThreadBuffer(
            [Dict{String,Any}[], Dict{String,Any}[]],
            [Base.ReentrantLock(), Base.ReentrantLock()],
            Threads.Atomic{Int}(1),
            Threads.Atomic{Int}(max_buffer),
            Threads.Atomic{Int}(0),
        ) for _ in 1:n
    ]
end

function _thread_shard_index(buffers)
    return mod1(Threads.threadid(), length(buffers))
end

function _ensure_stream_state()
    state = _stream_state[]
    if state === nothing
        buffers = _make_thread_buffers(max(1, Threads.nthreads()), DEFAULT_BUFFER_LIMIT)
        state = StreamState(nothing, buffers, nothing, Ref(false))
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

function _reset_buffers!(buffers)
    for shard in buffers
        for i in 1:2
            Base.lock(shard.locks[i])
            try
                empty!(shard.buffers[i])
            finally
                Base.unlock(shard.locks[i])
            end
        end
        shard.active_idx[] = 1
        shard.dropped[] = 0
    end
    return nothing
end

function _drain_shard!(shard::ThreadBuffer)
    old_idx = Threads.atomic_xchg!(shard.active_idx, shard.active_idx[] == 1 ? 2 : 1)
    Base.lock(shard.locks[old_idx])
    try
        events = copy(shard.buffers[old_idx])
        empty!(shard.buffers[old_idx])
        return events
    finally
        Base.unlock(shard.locks[old_idx])
    end
end

function _drain_buffers(buffers)
    events = Dict{String,Any}[]
    for shard in buffers
        shard_events = _drain_shard!(shard)
        if !isempty(shard_events)
            append!(events, shard_events)
        end
    end
    return events
end

function _append_event!(buffers, event)
    idx = _thread_shard_index(buffers)
    shard = buffers[idx]
    while true
        active = shard.active_idx[]
        Base.lock(shard.locks[active])
        try
            current = shard.active_idx[]
            if current != active
                continue
            end
            buffer = shard.buffers[active]
            if length(buffer) >= shard.max_buffer[]
                Threads.atomic_add!(shard.dropped, 1)
                return false
            end
            push!(buffer, event)
            return true
        finally
            Base.unlock(shard.locks[active])
        end
    end
end

function clear_trace!()
    stream = _stream_state[]
    if stream === nothing
        return nothing
    end
    stop_streaming!()
    _reset_buffers!(stream.buffers)
    stream.path = nothing
    return nothing
end

function snapshot_default_buffer()
    stream = _stream_state[]
    if stream === nothing
        return Dict{String,Any}[]
    end
    return _drain_buffers(stream.buffers)
end

function save_trace(path)
    p = String(path)
    events = snapshot_default_buffer()
    _write_json_array(p, events)
    return length(events)
end

function _append_stream_file(path, events; finalize::Bool=false)
    if isempty(events)
        return 0
    end

    mkpath(dirname(abspath(path)))

    separator = ",\n"
    if !isfile(path) || filesize(path) == 0
        separator = "[\n"
    end

    open(path, "a") do io
        write(io, separator)
        for (idx, event) in enumerate(events)
            if idx > 1
                write(io, ",\n")
            end
            write(io, JSON.json(event))
        end
    end

    if finalize
        open(path, "a") do io
            write(io, "\n]\n")
        end
    end

    return length(events)
end

function _flush_stream_state(stream::StreamState; finalize::Bool=false)
    if stream.path === nothing
        return 0
    end
    events = _drain_buffers(stream.buffers)
    if !isempty(events)
        return _append_stream_file(stream.path::String, events; finalize=finalize)
    end
    if finalize && isfile(stream.path)
        raw = read(stream.path, String)
        if !isempty(raw) && !endswith(raw, "]")
            open(stream.path, "a") do io
                write(io, "]")
            end
        elseif isempty(raw)
            open(stream.path, "w") do io
                write(io, "[]")
            end
        end
    end
    return 0
end

function total_dropped(stream::StreamState)
    total = 0
    for shard in stream.buffers
        Base.lock(shard.locks[1])
        Base.lock(shard.locks[2])
        try
            total += shard.dropped[]
        finally
            Base.unlock(shard.locks[2])
            Base.unlock(shard.locks[1])
        end
    end
    return total
end

function flush_trace!()
    stream = _stream_state[]
    if stream === nothing
        return 0
    end
    return _flush_stream_state(stream; finalize=true)
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
    if stream.path !== nothing
        _flush_stream_state(stream; finalize=true)
    end
    stream.path = nothing
    return nothing
end

function stream_trace(path; max_buffer=DEFAULT_BUFFER_LIMIT, flush_interval=DEFAULT_STREAM_FLUSH_INTERVAL)
    p = String(path)
    stream = _ensure_stream_state()
    if stream.task !== nothing
        stop_streaming!()
    end
    for shard in stream.buffers
        shard.max_buffer[] = max_buffer
    end
    stream.path = p
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
    _append_event!(stream.buffers, event)
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
