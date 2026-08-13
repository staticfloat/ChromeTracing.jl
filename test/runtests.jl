using Test
using JSON
using ChromeTracing

function reset_all!()
    clear_trace!()
end

@testset "ChromeTracing default buffer" begin
    reset_all!()
    for i in 1:10
        @tracepoint "default_event_$(i)" cat = "test"
    end
    path = joinpath(mktempdir(), "trace.json")
    saved = save_trace(path)
    @test saved == 10
    data = JSON.parsefile(path)
    @test length(data) == 10
    @test all(d["name"] == "default_event_$(i)" for (i, d) in enumerate(data))
end

@testset "ChromeTracing threaded generation" begin
    reset_all!()
    @sync for tid in 1:8
        Threads.@spawn begin
            for i in 1:100
                @tracepoint "thread_event_$(tid)_$(i)" cat = "threaded" ph = "i"
            end
        end
    end
    path = joinpath(mktempdir(), "threaded_trace.json")
    saved = save_trace(path)
    @test saved == 800
    data = JSON.parsefile(path)
    @test length(data) == 800
    @test all(haskey(d, "tid") for d in data)
    @test all(d["tid"] isa Int for d in data)
end

@testset "ChromeTracing block-form tracepoint emits B/E spans" begin
    reset_all!()
    @tracepoint "block_span" cat = "block" begin
        sleep(0.001)
    end
    path = joinpath(mktempdir(), "block_trace.json")
    saved = save_trace(path)
    @test saved == 2
    data = JSON.parsefile(path)
    @test length(data) == 2
    @test data[1]["name"] == "block_span"
    @test data[1]["ph"] == "B"
    @test data[2]["name"] == "block_span"
    @test data[2]["ph"] == "E"
end

@testset "ChromeTracing stream_trace drops when overloaded" begin
    reset_all!()
    path = joinpath(mktempdir(), "stream_trace.json")
    stream = stream_trace(path; max_buffer = 25, flush_interval = 0.25)
    for i in 1:500
        @tracepoint "stream_event_$(i)" cat = "stream" ph = "i"
    end
    sleep(0.05)
    @test total_dropped(stream) > 0
    written = flush_trace!()
    @test written <= 25
    data = JSON.parsefile(path)
    @test length(data) <= 25
    stop_streaming!()
end

@testset "ChromeTracing threaded stream writes valid Chrome trace" begin
    reset_all!()
    path = joinpath(mktempdir(), "threaded_stream_valid.json")
    stream = stream_trace(path; max_buffer = 5000, flush_interval = 0.02)

    workers = 6
    iterations = 250
    @sync for worker in 1:workers
        Threads.@spawn begin
            for i in 1:iterations
                @tracepoint "worker_$(worker)_event_$(i)" cat = "parallel" ph = "i" args = Dict("worker" => worker, "iteration" => i)
            end
            for i in 1:10
                @tracepoint "worker_$(worker)_span_$(i)" cat = "parallel" ph = "B" dur = 5
                @tracepoint "worker_$(worker)_span_$(i)" cat = "parallel" ph = "E"
            end
        end
    end

    sleep(0.05)
    stop_streaming!()

    @test isfile(path)
    data = JSON.parsefile(path)
    @test data isa Vector
    @test length(data) > 0
    @test all(haskey(d, "name") for d in data)
    @test all(haskey(d, "ts") for d in data)
    @test all(haskey(d, "tid") for d in data)
    @test all(d["name"] isa String for d in data)
    @test all(d["ts"] isa Int for d in data)
    @test all(d["pid"] isa Int for d in data)
    @test all(d["tid"] isa Int for d in data)
    raw = read(path, String)
    @test startswith(raw, "[")
    @test endswith(strip(raw), "]")
    @test total_dropped(stream) >= 0
end
