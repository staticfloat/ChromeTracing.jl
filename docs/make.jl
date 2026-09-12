using Documenter
using ChromeTracing

DocMeta.setdocmeta!(ChromeTracing, :DocTestSetup, :(using ChromeTracing); recursive=true)

makedocs(;
    modules=[ChromeTracing],
    sitename="ChromeTracing.jl",
    authors="Elliot Saba <staticfloat@gmail.com>",
    repo=Documenter.Remotes.GitHub("staticfloat", "ChromeTracing.jl"),
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical=get(ENV, "CI_PAGES_URL", nothing),
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "API reference" => "api.md",
        "Internals" => "internals.md",
    ],
    checkdocs=:exports,
    doctest=false,
)
