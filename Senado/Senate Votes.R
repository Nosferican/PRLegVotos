## This script scrappes the votes on legislative measures for PR Senate.
## Set Working Directory
setwd('/Users/jbsc/MEGAsync/PR/Senado')
## Load Packages
pacman::p_load(dplyr, data.table, dtplyr, plyr, pdftools, stringi, stringr, readr,
               doParallel, foreach)
## Parallel Processing
registerDoParallel(detectCores())
## Helpers
get_files = function() {
  files = read_csv(str_c('./', Cuatrienio, '/Diario de Sesiones.csv'),
                   col_names = FALSE, col_types = 'c') %>%
    unlist() %>%
    sapply(FUN = function(date) ifelse(test = nchar(date) == 5,
                                       yes = str_c('0', date), no = date))
  Date = files %>%
    str_extract_all(pattern = '\\d{6}') %>%
    unlist() %>%
    as.Date('%m%d%y')
  URL = files %>%
    lapply(FUN = function(file) {
      output = str_c('http://senado.pr.gov/Diario%20de%20Sesiones/',
                     file,
                     '.pdf')
      return(output)
    }) %>%
    unlist() %>%
    str_replace_all(pattern = ' ', replacement = '%20')
  output = data.table(URL = URL,
                      Date = Date) %>%
    filter(year(Date) >= as.integer(str_sub(Cuatrienio, start = 1, end = 4)),
           year(Date) <= as.integer(str_sub(Cuatrienio, start = 6, end = 9))) %>%
    setkey('Date') %>%
    getElement(name = 'URL')
  return(output)
}
get_leg_id = function() {
  output = read_csv(str_c('./', Cuatrienio, '/LegID.csv'), col_types = paste(rep(x = 'c', times = 7),
                                                                              collapse = '')) %>%
    data.table()
  return(output)
}
scrape = function(file) {
  ## Helpers
  Skip = function() {
    output = !(
      any(
        grepl(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES',
            x = text)))
    return(output)
  }
  NoHeaderNoFooter = function(page) {
    ## Strip whitespaces to single white space
    ## Gives a determined length to 'Total .... d' fill linewidth of 4
    ## No Header in form of 'Weekday, dd de mm de yyyy   Núm. dd'
    ## No Footer in form 'PageNumber'
    output = str_replace_all(string = page,
                             pattern = '\\s+',
                             replacement = ' ') %>%
      str_replace(pattern = '^\\w*,* \\d{1,2},* de \\w* de \\d{4} Núm\\.\\s*\\d* ',
                  replacement = '') %>%
      str_replace_all(pattern = '\\.{4,}',
                      replacement = '\\.\\.\\.') %>%
      str_replace(pattern = ' \\d* $', replacement = '')
    return(output)
  }
  get_pages = function(text) {
    starts = sapply(X = text, FUN = function(page) {
      grepl(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES',
            x = page)
    }) %>%
      which() %>%
      as.numeric()
    starts
    ends = sapply(X = text, FUN = function(page) {
      grepl(pattern = '(VOTOS ABSTENIDOS)', x = page)
    }) %>%
      which()
    ends = c(ends, ends + 1) %>%
      unique() %>%
      sort()
    ends = ends[c(which(diff(ends) > 1), length(ends))]
    output = lapply(1:length(starts), function(idx) {
      starts[idx]:min(ends[ends >= starts[idx]])
    }) %>%
      unlist()
    output = text[output]
    return(output)
  }
  extract_bill_id = function(text) {
    expander = function(idx) {
      if (length(output[[idx]]) > 0) {
        block = output[[idx]]
        output = vector(mode = 'character', length = sum(grepl(pattern = '[0-9]', block)))
        id = 1
        for (elem in block) {
          if (grepl(pattern = '[CKPRS]', x = elem)) {
            kind = elem
          } else {
            output[id] = paste0(kind,
                                str_pad(string = elem, width = 4, side = 'left', pad = '0'),
                                collapse = '')
            id = id + 1
          }}
        output = data.table(vote_id = idx,
                            bills_id = output) %>%
          setkey('vote_id')
        return(output)
      }
    }
    replacement = c('(Resolución|Resoluciones) de la Cámara' = 'R C ',
                    '((Resolución|Resoluciones) (del|de) Senado|S. R.)' = 'R S ',
                    'Senate Resolution' = 'R S ',
                    '(Plan|Planes) de Reorganización' = 'PRX ',
                    '(Proyecto|Proyectos) de (la|las) Cámara' = 'P C ',
                    '(Proyecto|Proyectos) del Senado' = 'P S ',
                    '(Resolución|Resoluciones) (Conjunta|Conjuntas) de la Cámara' = 'RCC ',
                    '(Resolución|Resoluciones) (Conjunta|Conjuntas) del Senado' = 'RCS ',
                    '(Resolución|Resoluciones) (Concurrente|Concurrentes) de la Cámara' =
                      'RKC ',
                    '(Resolución|Resoluciones) (Concurrente|Concurrentes) del Senado' =
                      'RKS ')
    output = text %>%
      str_replace_all(replacement) %>%
      str_replace_all(pattern = ' (?<=\\d{1,4} )\\d{4} \\(.*\\)', replacement = '') %>%
      str_replace_all(pattern = '(?<=\\d{1,4})\\s*de\\s*\\d{4}', replacement = '') %>%
      str_extract_all('(R C|R S|P R|PRX|P C|P S|RCC|RCS|RKC|RKS|\\d+)')
    # output
    output = lapply(X = 1:length(output), FUN = expander)
    output = do.call(what = rbind, args = output) %>%
      data.table(key = 'vote_id')
    return(output)
  }
  extract_vote_counts = function(text) {
    clean_leg = function(text, vote) {
      if (length(text) == 0) {
        return(data.table())
      }
      output = sapply(X = leg_id$KW, FUN = function(kw) {
        output = grepl(pattern = kw, text)
        return(output)
      })
      VecForm = is.vector(output)
      # results = sapply(X = text, FUN = function(each) {
      #   str_extract(str_sub(each, start = -2), '\\d+')
      #   }) %>%
      #   as.integer()
      # if (VecForm) {
      #   compare = sum(output)
      # } else {
      #   compare = rowSums(output)
      # }
      # sanitycheck = match(results, compare)
      # FirstFlag = (!(all.equal(results,compare)) & is.unsorted(sanitycheck))
      # SecondFlag = any(is.na(sanitycheck))
      # if (FirstFlag | SecondFlag) {
      #   print(str_c('It broke!', file, sep = ': '))
      # }
      if (VecForm) {
        output = leg_id$Leg_ID[output]
      } else {
        output = apply(X = output, MARGIN = 1, FUN = function(row) leg_id$Leg_ID[row])
      }
      if (length(output) == 0) {
        return(data.table())
      }
      if (VecForm) {
        output = data.table(vote_id = 1L,
                            leg_id = output,
                            vote = vote)
      } else {
        output = lapply(X = 1:length(output), FUN = function(idx) {
          if (length(output[[idx]]) > 0) {
            output = data.table(vote_id = idx,
                                leg_id = output[[idx]],
                                vote = vote)
            return(output)
          }
      })
        output = do.call(what = rbind, args = output)
      }
      return(output)
    }
    yes_votes = str_extract_all(string = text, pattern =
                                  'VOTOS AFIRMATIVOS .*?(Total*.*?\\d+)') %>%
      unlist() %>%
      str_replace_all(pattern = ':|,|\\.', replacement = '') %>%
      str_replace_all(pattern = '\\s*', replacement = '') %>%
      clean_leg(vote = 'Yes')
    no_votes = str_extract_all(text, pattern =
                                 'VOTOS NEGATIVOS .*?(Total.*?\\d+)') %>%
      unlist() %>%
      str_replace_all(pattern = ':|,|\\.', replacement = '') %>%
      str_replace_all(pattern = '\\s*', replacement = '') %>%
      clean_leg(vote = 'No')
    abstain_votes = str_extract_all(text, pattern =
                                      'VOTOS ABSTENIDOS .*?(Total.*?\\d+)') %>%
      unlist() %>%
      str_replace_all(pattern = ':|,|\\.', replacement = '') %>%
      str_replace_all(pattern = '\\s*', replacement = '') %>%
      clean_leg(vote = 'Abstain')
    output = rbind(yes_votes, no_votes, abstain_votes) %>%
      data.table(key = 'vote_id')
  }
  ## Constants
  Date = str_extract(string = file,
                     pattern = '\\d{6}') %>%
    as.Date('%m%d%y')
  text = file %>%
    url() %>%
    pdf_text()
  if (Skip()) {
    return(data.table())
  }
  text = text %>%
    get_pages() %>%
    sapply(FUN = NoHeaderNoFooter) %>%
    paste(collapse = '') %>%
    str_replace_all('\\s+', ' ') %>%
    str_extract(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES.*')
  bills_id = text %>%
    str_extract_all(pattern = '(?<=VOTACION|VOTACIÓN|VOTOS ABSTENIDOS).*?(?=:\\s*VOTOS AFIRMATIVOS)') %>%
    unlist() %>%
    str_replace_all(pattern = ', (es considerado|es considerada|
                    son considerados|es sometido) (a|en) Votación Final.*$',
                    replacement = '') %>%
    str_replace_all(pattern = '.*VOTACION', replacement = '') %>%
    str_replace_all(pattern = '^.*Núm\\.\\s*\\d+\\)',
                    replacement = '') %>%
    str_replace_all(pattern = '(?<=\\d{1,4}\\s{0,1}–{0,1}) \\d{4}\\s*\\(.*\\)', replacement = '') %>%
    str_replace_all(pattern = '^.*(Total|Tota).*\\.\\.\\. \\d+',
                    replacement = '') %>%
    str_replace_all(pattern = '^\\s*',
                    replacement = '') %>%
    extract_bill_id()
  if (nrow(bills_id) == 0) {
    return(data.table())
  }
  votes = text %>%
    extract_vote_counts()
  output = merge(x = bills_id, y = votes, allow.cartesian = TRUE) %>%
    ddply(.variables = 'bills_id', function(df) {
      output = df %>%
        filter(vote_id == max(vote_id))
      return(output)
    }) %>%
    mutate(Date = Date) %>%
    select(bills_id, Date, leg_id, vote)
  Absent = leg_id %>%
    filter(Date >= Start & Date <= End) %>%
    getElement(name = 'Leg_ID')
  output = ddply(.data = output, .variables = 'bills_id', .fun = function(df) {
    missing = setdiff(x = Absent, y = df$leg_id)
    if (length(missing) > 0) {
      toadd = with(df, data.table(bills_id = unique(bills_id),
                                  Date = unique(Date),
                                  leg_id = missing,
                                  vote = 'Absent'))
      output = rbind(df, toadd)
      return(output)
    } else {
      return(df)
    }
    })
  return(output)
  }
chk = function(file) {
  ## Helpers
  Skip = function() {
    output = !(
      any(
        grepl(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES',
              x = text)))
    return(output)
  }
  NoHeaderNoFooter = function(page) {
    ## Strip whitespaces to single white space
    ## Gives a determined length to 'Total .... d' fill linewidth of 4
    ## No Header in form of 'Weekday, dd de mm de yyyy   Núm. dd'
    ## No Footer in form 'PageNumber'
    output = str_replace_all(string = page,
                             pattern = '\\s+',
                             replacement = ' ') %>%
      str_replace(pattern = '^\\w*, \\d{1,2} de \\w* de \\d{4} Núm\\.\\s*\\d* ',
                  replacement = '') %>%
      str_replace_all(pattern = '\\.{4,}',
                      replacement = '\\.\\.\\.') %>%
      str_replace(pattern = ' \\d* $', replacement = '')
    return(output)
  }
  get_pages = function(text) {
    starts = sapply(X = text, FUN = function(page) {
      grepl(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES',
            x = page)
    }) %>%
      which() %>%
      as.numeric()
    starts
    ends = sapply(X = text, FUN = function(page) {
      grepl(pattern = '(VOTOS ABSTENIDOS)', x = page)
    }) %>%
      which()
    ends = c(ends, ends + 1) %>%
      unique() %>%
      sort()
    ends = ends[c(which(diff(ends) > 1), length(ends))]
    output = lapply(1:length(starts), function(idx) {
      starts[idx]:min(ends[ends >= starts[idx]])
    }) %>%
      unlist()
    output = text[output]
    return(output)
  }
  extract_vote_counts = function(text) {
    fixer = function(string) {
      output = string %>%
        str_replace(pattern = '^.*:\\s*', replacement = '') %>%
        str_replace_all(pattern = '(VOTOS NEGATIVOS)|(VOTOS ABSTENIDOS)', replacement = '') %>%
        str_replace(pattern = '\\.*\\s*Total\\s*\\.+\\s*\\d*\\s*$', replacement = '') %>%
        str_replace_all(pattern = '(Presidente)|(Presidenta)|(Accidental)|
                        (Vicepresidente)|(Vicepresidenta)|
                        (\\.*)', replacement = '') %>%
        str_split(pattern = ',|( y )|( e )') %>%
        unlist() %>%
        str_replace_all(pattern = '(\\.)|(\\s*)', replacement = '')
      return(output)
    }
    yes_votes = text %>%
      str_extract_all(pattern = 'VOTOS AFIRMATIVOS .*?(Total*.*?\\d+)') %>%
      unlist() %>%
      sapply(fixer) %>%
      unlist() %>%
      unique()
    no_votes = str_extract_all(text, pattern =
                                 'VOTOS NEGATIVOS .*?(Total.*?\\d+)') %>%
      unlist() %>%
      sapply(fixer) %>%
      unlist() %>%
      unique()
    abstain_votes = str_extract_all(text, pattern =
                                      'VOTOS ABSTENIDOS .*?(Total.*?\\d+)') %>%
      unlist() %>%
      sapply(fixer) %>%
      unlist() %>%
      unique()
    output = c(yes_votes, no_votes, abstain_votes) %>%
      unique() %>%
      data.table() %>%
      setnames(old = '.', new = 'leg_id') %>%
      filter(leg_id != '')
  }
  ## Constants
  Date = str_extract(string = file,
                     pattern = '\\d{6}') %>%
    as.Date('%m%d%y')
  text = file %>%
    url() %>%
    pdf_text()
  if (Skip()) {
    return(data.table())
  }
  text = text %>%
    get_pages() %>%
    sapply(FUN = NoHeaderNoFooter) %>%
    paste(collapse = '') %>%
    str_replace_all('\\s+', ' ') %>%
    str_extract(pattern = 'CALENDARIO DE APROBACION FINAL DE PROYECTOS DE LEY Y RESOLUCIONES.*')
  votes = text %>%
    extract_vote_counts()
}
## Choose Term
Cuatrienio = '2009-2012'
## Get Files to Parse
files = get_files()
## Get Legislators
leg_id = get_leg_id()
## Run the script to get output

scrape(files)

Start = Sys.time()
Start
Votos1 = foreach(file = files[1:100], .combine = rbind) %dopar% scrape(file)
End = Sys.time() - Start
print(End)
Sys.sleep(12)
Start = Sys.time()
Start
Votos2 = foreach(file = files[101:200], .combine = rbind) %dopar% scrape(file)
End = Sys.time() - Start
print(End)
Sys.sleep(15)
Start = Sys.time()
Start
Votos3 = foreach(file = files[201:300], .combine = rbind) %dopar% scrape(file)
End = Sys.time() - Start
print(End)
Sys.sleep(15)
Start = Sys.time()
Start
Votos4 = foreach(file = files[301:319], .combine = rbind) %dopar% scrape(file)
End = Sys.time() - Start
print(End)

Votos = do.call(what = rbind,
                args = list(Votos1,
                            Votos2,
                            Votos3,
                            Votos4))

chk = scrape(files[201 + 42])

VotosX = Votos %>%
  filter(grepl(pattern = 'PRX', x = bills_id)) %>%
  select(bills_id, Date) %>%
  unique()

write_csv(VotosX, '2009-2012/VotosX.csv')

PRFix = read_csv(file = '2009-2012/PRFix.csv', col_types = 'ccc') %>%
  mutate(Standard = ifelse(test = Body == 'C',
                      yes = str_c('PRC', str_sub(Medida, start = 4)),
                      no = str_c('PRS', str_sub(Medida, start = 4)))) %>%
  mutate(Fecha = as.Date(Fecha, '%Y-%m-%d')) %>%
  select(-Body) %>%
  data.table()
Votos2 = Votos %>%
  filter(grepl(pattern = 'P R', x = Medida))
Votos3 = merge(Votos2, PRFix, by = c('Medida','Fecha'), all.x = TRUE)
Votos3 = Votos3 %>%
  filter(complete.cases(Votos3)) %>%
  select(-Medida) %>%
  data.table() %>%
  setnames(old = 'Standard', new = 'Medida') %>%
  setcolorder(neworder = c('Medida','Fecha','Senador','Votación'))

Votos = Votos %>%
  filter(!grepl(pattern = 'P R', x = Medida)) %>%
  rbind(Votos3)

VotosSenado0912 = Votos %>%
  setnames(old = 'bills_id', new = 'Medida') %>%
  setnames(old = 'Date', new = 'Fecha') %>%
  setnames(old = 'leg_id', new = 'Senador') %>%
  setnames(old = 'vote', new = 'Votación')
chk = table(VotosSenado0912$Medida)
chk[chk > 150]
table(chk)
chk = scrape(files[58])

Votos = data.table()
for (file in files) {
  Votos = rbind(Votos, chk(file))
}
Votos = Votos %>%
  unique()
Votos = Votos %>%
  mutate(Fixed = str_replace_all(string = leg_id, pattern = '(Total)|(\\d*)', replacement = '') %>%
           str_replace_all(pattern = '…', replacement = ''))
Votos2 = Votos %>%
  select(Fixed) %>%
  filter(Fixed != '') %>%
  unique()
write_csv(Votos2, '2009-2012/kw.csv')
Vototxs = sapply(files[1:15], chk) %>%
Votos = sapply(Votos, function(vec) data.table(leg_id = vec))


Votos = Votos %>%
  unique()

chk2 = unique(Votos)
chk3 = chk2$. %>%
  str_split(' y ') %>%
  unlist() %>%
  trimws() %>%
  unique() %>%
  str_replace_all('resident.*\\d*','') %>%
  unlist() %>%
  str_replace_all('Total.*\\d+','') %>%
  unlist() %>%
  unique() %>%
  str_replace_all(' ','') %>%
  unique() %>%
  data.table()

write_csv(chk3, str_c('./', Cuatrienio, '/kw.csv'))
## Run the script to get output
Start = Sys.time()
Start
Votos = foreach(file = files, .combine = rbind) %dopar% scrape(file)
End = Sys.time() - Start
End

## saveRDS(object = Votos, file = 'VotosSenado09-16.RDS')
Leg = leg_id$leg_id
Formal = leg_id$Formal
Votos = readRDS(file = 'VotosSenado09-16.RDS')
Votos = Votos %>%
  mutate(leg_id = str_replace_all(string = leg_id,
                                  pattern = 'Antonio J. Fas Alzamor',
                                  replacement = 'Antonio J. Fas Alzamora')) %>%
  mutate(leg_id = mapvalues(x = leg_id, from = Leg, to = Formal))
x = Votos %>%
  mutate(bills_id = str_replace_all(string = bills_id,
                                    pattern = 'PR2010',
                                    replacement = 'PR3'),
         Session = ifelse(test = year(Date) <= 2012,
                          yes = '2009 - 2012',
                          no = '2013 - 2016'),
         Stage = ifelse(test = Date <= as.Date(x = '2011-08-28', '%Y-%m-%d'),
                        yes = 1L,
                        no = ifelse(test = year(Date) < 2013,
                                    yes = 2,
                                    no = 3)))
FirstStage = x %>%
  filter(Date <= as.Date(x = '2011-08-28', '%Y-%m-%d')) %>%
  getElement(name = 'leg_id') %>%
  unique()
SecondStage = x %>%
  filter(Date > as.Date(x = '2011-08-28', '%Y-%m-%d') &
           year(Date) <= 2012) %>%
  getElement(name = 'leg_id') %>%
  unique()
ThirdStage = x %>%
  filter(year(Date) >= 2013) %>%
  getElement(name = 'leg_id') %>%
  unique()
Stages = data.table(leg_id = c(FirstStage,SecondStage,ThirdStage),
                    Session = c(rep(x = 1L, length(FirstStage)),
                                rep(x = 2L, length(SecondStage)),
                                rep(x = 3L, length(ThirdStage))))
lst = split(x, apply(select(x, bills_id, Date), MARGIN = 1,
                     function(row) paste(row[1], row[2])))
backup = leg_id
helper = function(df) {
  bills_id = unique(df$bills_id)
  Date = unique(df$Date)
  Session = unique(df$Session)
  Stage = unique(df$Stage)
  vote = 'Absent'
  leg_id = Stages %>%
    filter(Stage == Stage) %>%
    getElement(name = 'leg_id') %>%
    setdiff(df$leg_id)
  output = rbind(df, data.frame(bills_id = bills_id,
                                Date = Date,
                                leg_id = leg_id,
                                vote = vote,
                                Session = Session,
                                Stage = Stage))
  return(output)
  }
output = foreach(idx = 1:length(lst), .combine = rbind) %dopar% helper(lst[[idx]])
output = output %>%
  mutate(vote = mapvalues(vote, from = c('Yes','No','Abstain','Absent'),
                          to = c('A favor', 'En contra', 'Abstenido', 'Ausente'))) %>%
  setnames(old = 'bills_id', 'Medida') %>%
  setnames(old = 'Date', 'Fecha') %>%
  setnames(old = 'vote', 'Voto') %>%
  setnames(old = 'Session', 'Cuatrienio')
output2 = output %>%
  # setnames(old = 'leg_id', 'Senador') %>%
  setcolorder(c('Cuatrienio','Medida','Fecha','Legislador','Voto','Session'))
output3 = output2 %>%
  setnames('Legislador','Senador')
Votos09_16 = write_csv(x, 'Senado_09_16.csv')

Votos09_16 = read_csv('Senado_09_16.csv', col_types = 'ccDcc')
x = Votos09_16 %>%
  mutate(Cuerpo = str_extract(string = Medida, pattern = '[C|S](?=\\d)'),
         Tipo = str_extract(string = Medida, pattern = '[C|K|P|R]{1,2}?(?=[C|S|\\d])') %>%
           str_pad(width = 2L, side = 'right', pad = ' '),
         Número = str_extract(string = Medida, pattern = '\\d{1,4}') %>%
           str_pad(width = 4L, side = 'left', pad = '0')) %>%
  mutate(Cuerpo = ifelse(test = is.na(Cuerpo), yes = 'X', no = Cuerpo),
         Medida = str_c(Tipo, Cuerpo, Número)) %>%
  select(Cuatrienio, Medida, Fecha, Senador, Voto)

x = x %>%
  mutate(Medida = str_replace_all(string = Medida,
                                  pattern = '(?<=[A-Z]{1,3}).*?(?=\\d{2})',
                                  replacement = '00'))
x = x %>%
  mutate(Medida = str_replace_all(string = Medida,
                                  pattern = '(?<=[A-Z]{1,3}).*?(?=\\d{3})',
                                  replacement = '0'))
x = Votos09_16 %>%
  mutate(Medida = str_replace_all(string = Medida,
                                  pattern = '(?<=[A-Z]).*?(?=[A-Z]\\d+)',
                                  replacement = ' '))

ddply(.variables = c('bills_id', 'Session'), .fun = function(df) {
    output = df %>%
      filter(Date == max(Date))
  })
