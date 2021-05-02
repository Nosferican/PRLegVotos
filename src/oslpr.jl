using WebDriver

ENV["WEBDRIVER_HOST"] = get(ENV, "WEBDRIVER_HOST", "localhost")
ENV["WEBDRIVER_PORT"] = get(ENV, "WEBDRIVER_PORT", "4444")

capabilities = Capabilities("chrome")
wd = RemoteWebDriver(
    capabilities,
    host = ENV["WEBDRIVER_HOST"],
    port = parse(Int, ENV["WEBDRIVER_PORT"]),
    )
# New Session
session = Session(wd)

navigate!(session, "https://sutra.oslpr.org/osl/esutra/MedidaBus.aspx")
using Base64
using Base64
ss = write(joinpath(@__DIR__, "img.png"), base64decode(screenshot(session)))

#how about?
fi = "c:\\temp\\myfile.png"
function screenshot(fi,session) 
    ss = write(fi, base64decode(screenshot(session)))
    return ss
end
using Base64
ss = write(joinpath("img.png"), base64decode(screenshot(session)))
