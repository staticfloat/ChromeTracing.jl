# ChromeTracing.jl

Package for tracing Julia programs and writing those traces out as Chrome Trace Viewer JSON files.

## Quick start

```julia
using ChromeTracing

stream_trace("trace.json"; flush_interval=0.05)

@tracepoint "startup" cat="app" args=Dict("msg" => "boot")

@tracepoint "work" cat="compute" begin
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

## Documentation

Full API documentation is built with [Documenter](https://documenter.juliadocs.org/)
and published to GitLab Pages by CI from the default branch.

To build it locally:

```bash
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

The rendered site lands in `docs/build/`.

## Development

Run the test suite:

```bash
julia --project=. --threads=4 -e 'using Pkg; Pkg.test()'
```

CI is configured in [`.gitlab-ci.yml`](.gitlab-ci.yml) and runs the tests against
Julia 1.10, 1.11 and 1.12 (plus `latest` as a non-blocking job), reports
coverage, and builds and deploys the docs.
