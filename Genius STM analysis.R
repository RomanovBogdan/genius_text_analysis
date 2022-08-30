library(quanteda)
library("readxl")
library(stringr)
library(stm)

# load article data
dat <- read_excel("lyrics_20182022.xlsx")

## 'tokenize' fulltext
texts <- tokens(dat$lyrics_without_stopwords, what = "word",
                #remove_numbers = T,
                remove_punct = T,
                remove_symbols = T,
                remove_separators = T,
                remove_hyphens = T,
                remove_url = T,
                verbose = T)

texts <- tokens_tolower(texts)
texts <- tokens_remove(texts, stopwords("english"))
texts <- tokens_wordstem(texts)
texts <- tokens_remove(texts, stopwords("english"))

# get actual dfm from tokens
tm <- dfm(texts)

# check out top-appearing features in dfm
topfeatures(tm)

# keep features (words) appearing in >2 documents
tm <- dfm_trim(tm, min_termfreq = 4)

# filter out one-character words
tm <- tm[, str_length(colnames(tm)) > 2]

# Create tf_idf-weighted dfm
ti <- dfm_tfidf(tm)

# Select from main dfm using its top features
tm <- dfm_keep(tm, names(topfeatures(ti, n = 1000)))

content.vars <- data.frame(
  before = dat$COVID == "Before",
  after = dat$COVID == "After"
)

# run lda with 4 topics
fit_before <- stm(documents = tm, 
           K = 10,
           data = content.vars)

summary(fit_before)

plot(fit_before)

# topic associations with country dummies
est <- estimateEffect(1:4 ~ before + after, fit_before, content.vars)