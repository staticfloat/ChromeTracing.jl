using Base.Threads
using ChromeTracing

# This example produces a Chrome-trace-compatible JSON file.
# You can run it with:
#   julia --project=. example.jl

output_path = joinpath(@__DIR__, "example_trace.json")
rm(output_path; force=true)

# Start a background streaming writer.
stream_trace(output_path; capacity = 5000, flush_interval = 0.05)

# Emit a few scalar events.
@trace_event "app.start" cat = "app" ph = "i" args = Dict("message" => "service started")

@sync for worker in 1:6
    Threads.@spawn begin
        for i in 1:60
            @trace_event "worker_$(worker)_tick" cat = "worker" ph = "i" args = Dict("worker" => worker, "iteration" => i)
            sleep(0.0001)
        end

        for i in 1:12
            @trace_event "worker_$(worker)_span1_$(i)" cat = "worker" begin
                @trace_event "worker_$(worker)_span2_$(i)" cat = "worker" begin
                    sleep(0.001)
                    @trace_event "worker_$(worker)_span3_$(i)" cat = "worker" begin
                        sleep(0.001)
                    end
                end
                sleep(0.001)
            end
            sleep(0.003)
        end
    end
end

# A more explicit span pair.
@trace_event "render.begin" cat = "ui" ph = "B" args = Dict("screen" => "main")
@trace_event "render.end" cat = "ui" ph = "E"

# Flush remaining data to disk.
stop_streaming!()

println("Wrote trace JSON to: $(output_path)")
println("File size: $(filesize(output_path)) bytes")
