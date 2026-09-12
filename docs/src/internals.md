```@meta
CurrentModule = ChromeTracing
```

# Internals

These are implementation details of the event buffer. They are not part of the
public API and may change without a breaking release.

## Buffer state

```@docs
StreamState
```

## Buffer operations

```@docs
_make_stream_state
_ensure_stream_state
_append_event!
_drain_buffer!
_reset_buffer!
_flush_stream_state
```

## Helpers

```@docs
_now_ts
_normalize_args
_coerce_trace_key
_write_json_array
```
