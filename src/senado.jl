using PDFIO, Dates, DataFrames, CSV
using PDFIO: PD.PDDocImpl, PD.PDPageImpl

const MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
doc = pdDocOpen(joinpath("data", "senado", "2011_06_30_2011_09_01.pdf"))
npage = pdDocGetPageCount(doc)
i = 1
page = pdDocGetPage(doc, i)
io = IOBuffer()
pdPageExtractText(io, page)
text = String(take!(io))
lns = split(text, '\n')
medida, resultado = split(strip(lns[3]), r"\s{2,}")
fecha = match(r"(?<=, )\d{1,2} de \w+ de \d{4}", lns[4]).match |>
    (obj -> Date(parse(Int, match(r"\d{4}$", obj).match),
                 findfirst(isequal(match(r"(?<=de )\w+(?= de)", fecha).match), MONTHS),
                 parse(Int, match(r"\d{1,2}", obj).match)))
votos = split.(strip.(lns[findfirst(x -> occursin("Votante", x), lns) + 1:end]), r"\s{2,}")
first.(votos)
last.(votos)
data = DataFrame(fecha = fecha, medida = medida, votante = first.(votos), voto = last.(votos))

"""
    senate_rollcall(page::PDPageImpl)
"""
function senate_rollcall(page::PDPageImpl)
    io = IOBuffer()
    pdPageExtractText(io, page)
    text = String(take!(io))
    lns = split(text, '\n')
    medida = strip(match(r"^.*(?=(Resultado Recibido|\s{2,}|$))", lns[3]).match)
    fecha = match(r"(?<=, )\d{1,2} de \w+ de \d{4}", lns[findfirst(ln -> occursin(r"\d{1,2} de \w+ de \d{4}", ln), lns)]).match |>
        (obj -> Date(parse(Int, match(r"\d{4}$", obj).match),
                     findfirst(isequal(match(r"(?<=de )\w+(?= de)", obj).match), MONTHS),
                     parse(Int, match(r"\d{1,2}", obj).match)))
    votos = split.(strip.(lns[findfirst(x -> occursin("Votante", x), lns) + 1:end]), r"\s{2,}")
    DataFrame(fecha = fecha, medida = medida, votante = first.(votos), voto = last.(votos))
end
"""
    senate_rollcall(doc::PDDocImpl)
"""
function senate_rollcall(doc::PDDocImpl)
    npage = pdDocGetPageCount(doc)
    output = DataFrame()
    for i in 1:pdDocGetPageCount(doc)
        # i = 3
        page = pdDocGetPage(doc, i)
        append!(output, senate_rollcall(page))
    end
    output
end
output = DataFrame()
ts_start = now()
for file in readdir(joinpath("data", "senado"), join = true)
    append!(output, senate_rollcall(pdDocOpen(file)))
end
ts_end = now()
Dates.canonicalize(Dates.CompoundPeriod(ts_end - ts_start))
output[!,:medida] .= replace.(output[!,:medida], r"  .*" => "")
CSV.write(joinpath("data", "senado.csv"), sort!(output))
