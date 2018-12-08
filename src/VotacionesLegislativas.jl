module PRLegVotos

# Bibliotecas
using CSV, Dates, DataFrames, HTTP, SQLite, PDFIO, Match

# Base de datos
db = SQLite.DB("data/2013-2016.db")
# Crear la tabla Cámara
# SQLite.Query(db, "drop table Cámara")
SQLite.Query(db, "create table Cámara (
                  tipo char(3),
                  número char(4),
                  fecha date,
                  votante varchar,
                  voto varchar);")
# Crear la tabla Senado
# SQLite.Query(db, "drop table Senado")
SQLite.Query(db, "create table Senado (
                  tipo char(3),
                  número char(4),
                  fecha date,
                  votante varchar,
                  voto varchar);")

# Globales
const abreviaciones = ("P. de la C." => "P C",
                       "P. del S." => "P S",
                       "R. de la C." => "R C",
                       "R. del S." => "R S",
                       "R. C. de la C." => "RCC",
                       "R. C. del S." => "RCS",
                       "R. C. de la C." => "RCC",
                       "R. C. del S." => "RCS",
                       "R. Conc. de la C." => "RKC",
                       "R. Conc. del S." => "RKS",
                       "Nombramiento" => "N S",
                       "PC" => "P C",
                       "PS" => "P S",
                       "S. R." => "R S")
const dividers = reduce((x, y) -> "$x|$y", string.("(", last.(abreviaciones), ")")) |>
    (x -> Regex("\\s(?=($x))"))
# Auxiliares
"""
    meses(mes)

Asigna el número del mes al mes en español.
"""
function meses(mes)
    @match mes begin
        "enero" => 1
        "febrero" => 2
        "marzo" => 3
        "abril" => 4
        "mayo" => 5
        "junio" => 6
        "julio" => 7
        "agosto" => 8
        "septiembre" => 9
        "octubre" => 10
        "noviembre" => 11
        "diciembre" => 12
    end
end

"""
    medidas(medida)

Separa las varias medidas.
"""
function medidas(medida)
    for abreviación in abreviaciones
        medida = replace(medida, abreviación)
    end
    medida = split(medida, dividers)
    if isa(medida, AbstractString)
        output = m[1:3] .* getproperty.(eachmatch(r"\d{4}", m), :match)
    else
        output = String[]
        for m in medida
            append!(output, m[1:3] .* getproperty.(eachmatch(r"\d{4}", m), :match))
        end
    end
    output
end

# Cámara de Representantes

# En data/Cámara debe de haber un archívo con measureid and voteid
"""
    votaciones_de_la_cámara!()

Updates the data/Cámara directory with all the HTML based on
data/Cámara/cámara.tsv
"""
function votaciones_de_la_cámara!()
    for id ∈ eachrow(CSV.read("data/Cámara/cámara.tsv", delim = '\t'))
        file = "data/Cámara/$(id.measureid)-$(id.voteid).html"
        if ~isfile(file)
            link = string("http://www.tucamarapr.org/dnncamara/web/actividadlegislativa/",
                          "votaciones.aspx?measureid=",
                          id.measureid,
                          "&voteid=",
                          id.voteid)
            response = HTTP.request("GET", link)
            response.status == 200 || println("$row failed")
            write("data/Cámara/$(id.measureid)-$(id.voteid).html", String(response.body))
        end
    end
end
votaciones_de_la_cámara!()

# Para acutalizar data/Cámara/cámara.tsv
# parse_legid(data, idx) =
#     (measureid = match(r"(?<=measureid=).*(?=&)", data[idx + 2]).match,
#      voteid = match(r"(?<=voteid=).*(?=')", data[idx + 2]).match)
# cámara = "data/cámara.html"
# data = readlines(cámara) |>
#     (data -> findall(x -> occursin(r"\s+<span class=\"measure-name\">", x), data) |>
#         (x -> parse_legid.(Ref(data), x)))
# CSV.write("data/Cámara/cámara.tsv", sort!(data), delim = '\t')

"""
    procesar_camara!(html)

Writes data from html to the database.
"""
function procesar_camara!(html)
    lines = readlines(html)
    tipo, número = strip(lines[328]) |>
        (x -> (x[1:3], x[4:7]))
    fecha = strip(lines[354]) |>
        (x -> parse.(Int, split(x, '/'))) |>
        (x -> Date(x[3], x[1], x[2])) |>
        string
    for idx ∈ findall(x -> occursin(r"Hon\..*", x), lines)
        votante = strip(lines[idx])
        voto = strip(lines[idx + 3])
        voto = voto == "" ? "Abstenido" : voto
        SQLite.Query(db,
                     "INSERT INTO Cámara VALUES (" *
                     "'$tipo', '$número', '$fecha', '$votante', '$voto')")
    end
end
for html ∈ readdir("data/Cámara")
    if endswith(html, ".html")
        procesar_camara!("data/Cámara/$html")
    end
end

# Senado

@views function procesar_senado!(líneas)
    medida = reduce((x, y) -> "$x $y",
                    findfirst(x -> occursin("Resultado", x), líneas[3:end]) |>
                    (idx -> strip.(líneas[3:idx + 2]))) |>
                    medidas |>
                    (medidas -> [ (m[1:3], m[4:7]) for m ∈ medidas ])
    fecha = findfirst(x -> occursin(r"\d+ de \w+ de \d{4}", x), líneas) |>
        (idx -> match(r"\d+ de \w+ de \d{4}", líneas[idx]).match) |>
        (fecha -> Date(parse(Int, fecha[end - 4:end]),
                       meses(match(r"(?<=de )\w+(?= de)", fecha).match),
                       parse(Int, match(r"^\d+", fecha).match)))
    for voto ∈ líneas[findfirst(x -> occursin("Votante", x), líneas) + 1:end - 1]
        votante, voto = split(strip(voto), r"\s{2,}")
        for (t, n) ∈ medida
            SQLite.Query(db,
                         "INSERT INTO Senado VALUES ('$t', '$n', '$fecha', " *
                         "'Hon. $votante', '$voto')")
        end
    end
end

for pdf in readdir("data/Senado")
    if endswith(pdf, ".pdf")
        doc = pdDocOpen("data/Senado/$pdf")
        for i ∈ 1:pdDocGetPageCount(doc)
            pdDocGetPage(doc, i) |>
            (page -> pdPageExtractText(IOBuffer(), page)) |>
            (io -> readlines(IOBuffer(String(take!(io))))) |>
            procesar_senado!
        end
        pdDocClose(doc)
    end
end

end
