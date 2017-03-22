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
    str_replace_all(pattern = '(?<=\\d{1,4}) \\d{4}\\s*\\(.*\\)', replacement = '') %>%
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

## Choose Term
Cuatrienio = '2009-2012'
## Get Files to Parse
files = get_files()
## Get Legislators
## leg_id contains the mannually checked composition of the body and any changes to it. It also contains the formal PaternalSurname MaternalSurname, FirstName MiddleInicial. Lastly it has the keyword that uniquely identifies all instances of the leg_id in the relevant sections after being cleaned and transformed.
leg_id = get_leg_id()
## Run the script to get output
## Limit each scrape to 100 files to avoid hitting the servers too hard
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
## VotosX gives all Programa de Reorganización type of bills and the dates of the votes
VotosX = Votos %>%
  filter(grepl(pattern = 'PRX', x = bills_id)) %>%
  select(bills_id, Date) %>%
  unique()
write_csv(VotosX, '2009-2012/VotosX.csv')
## For now, manually check the bill_id number and date of the vote to find out whether it is from the Senate or the House.
## PRFix has the information of to which body the bill refers to.
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
