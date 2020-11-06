using HTTP: request
using Cascadia: parsehtml, Selector, getattr, nodeText, tag
using DataFrames: DataFrame
using Dates: Date
using CSV: CSV

html = parsehtml(String(read(joinpath("data", "cámara", "Votaciones.html"))))

sel = Selector("span.measure-name > a")
medidas = [ string("http://www.tucamarapr.org/dnncamara/web/actividadlegislativa/", getattr(x, "href")) for x in eachmatch(sel, html.root) ]
# files = string.(SubString.(medidas, range.(78, lastindex.(medidas), step = 1)), ".html")
# filter!(file -> file ∉ readdir(joinpath("data", "cámara")), files)

# response = request("GET",
                #    string("http://www.tucamarapr.org/dnncamara/web/actividadlegislativa/", SubString(files[1], range(1, lastindex(files[1]) - 5))))

for medida in medidas
    filepath = string(SubString(medida, 78:lastindex(medida)), ".html")
    if !isfile(joinpath("data", "cámara", filepath))
        try
            response = request("GET", medida)
            println(filepath)
            write(joinpath("data", "cámara", filepath), String(response.body))
        catch err
            println(medida)
        end
    end
end
files = filter!(x -> occursin("measureid=", x), readdir(joinpath("data", "cámara"), join = true))

"""
    house_rollcall(file)
"""
function house_rollcall(file)
    html = parsehtml(String(read(file)))
    sel = Selector("#content")
    data = eachmatch(sel, html.root)
    medida = strip(nodeText(data[2][2][1][1].children[4]))
    fecha = Date(strip(nodeText(data[2][2][1][3].children[end])), "m/d/y")
    data = DataFrame((Representante = strip(nodeText(x[1])), Votación = strip(nodeText(x[2])))
                     for x in only(filter(x -> tag(x) == :table, data[2][3][1][2].children))[1].children[2:end])
    data[!,:medida] .= medida
    data[!,:fecha] .= fecha
    data[!,[:fecha, :medida, :Representante, :Votación]]
end
output = DataFrame()
for file in files
    try
        append!(output, house_rollcall(file))
    catch err
        println(file)
        # rm(file)
    end
end
# file = "data/cámara/measureid=53598&voteid=474.html"
CSV.write(joinpath("data", "cámara.csv"), sort!(output))
