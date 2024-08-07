library(argparser)
library(assertthat)
library(rlang)
library(data.table)
library(vctrs)
library(yaml)
library(pryr)

source("src/misc.R")
source("src/steps.R")
source("src/sequential.R")
source("src/obs_time.R")


# Create a parser
p <- arg_parser("Extract and preprocess ICU mortality data")
p <- add_argument(p, "--src", help="source database", default="mimic_demo")
argv <- parse_args(p)

src <- argv$src 
conf <- yaml.load_file("../config.yaml")
path <- file.path(conf$out_dir, "base")


cncpt_env <- new.env()

# Task description
time_flow <- "sequential" # sequential / continuous
time_unit <- hours
freq <- 1L
max_len <- 7 * 24  # = 7 days

static_vars <- c("age", "sex", "ethnic", "adm", "los_icu", "los_hosp")

dynamic_vars <- c("alb", "alp", "alt", "ast", "be", "bicar", "bili", "bili_dir",
                  "bnd", "bun", "ca", "cai", "ck", "ckmb", "cl", "crea", "crp", 
                  "dbp", "fgn", "fio2", "glu", "hgb", "hr", "inr_pt", "k", "lact",
                  "lymph", "map", "mch", "mchc", "mcv", "methb", "mg", "na", "neut",
                  "o2sat", "pco2", "ph", "phos", "plt", "po2", "ptt", "resp", "sbp", 
                  "temp", "tnt", "urine", "wbc")

# cross-sectional vs longitudinal
predictor_type <- "dynamic" # static / dynamic
outcome_type <- NULL



patients <- stay_windows(src, interval = time_unit(freq))
patients <- as_win_tbl(patients, index_var = "start", dur_var = "end", interval = time_unit(freq))


# Define outcome ----------------------------------------------------------

# No outcome for the base cohort, which is meant to describe differences between 
# databases. 



# Define observation times ------------------------------------------------

stop_obs_at(patients, offset = ricu:::re_time(hours(max_len), time_unit(freq)), by_ref = TRUE)

# Apply exclusion criteria ------------------------------------------------
print("apply exclusion critera")
print("apply exclusion critera: Los")
# 1. Invalid LoS
excl1 <- patients[end < 0, id_vars(patients), with = FALSE]
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
# 2. Stay <6h
x <- load_step("los_icu")
x <- filter_step(x, ~ . < 6 / 24)

print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
excl2 <- unique(x[, id_vars(x), with = FALSE])

print("apply exclusion critera: less than 4 measurements")
# 3. Less than 4 measurements
n_obs_per_row <- function(x, ...) {
  # TODO: make sure this does not change by reference if a single concept is provided
  obs <- data_vars(x)
  x[, n := as.vector(rowSums(!is.na(.SD))), .SDcols = obs]
  x[, .SD, .SDcols = !c(obs)]
}

x <- load_step(dict[dynamic_vars], interval=time_unit(freq), cache = TRUE)
x <- summary_step(x, "count", drop_index = TRUE)
x <- filter_step(x, ~ . < 4)

excl3 <- unique(x[, id_vars(x), with = FALSE])

print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
rm(x)
gc()
print(paste("Memory used after cleanup:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))

print("apply exclusion critera: gaps")
# 4. More than 12 hour gaps between measurements
map_to_grid <- function(x) {
  grid <- ricu::expand(patients)
  merge(grid, x, all.x = TRUE)
}

longest_rle <- function(x, val) {
  x <- x[, rle(.SD[[data_var(x)]]), by = c(id_vars(x))]
  x <- x[values != val, lengths := 0]
  x[, .(lengths = max(lengths)), , by = c(id_vars(x))]
}
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
x <- load_step(dict[dynamic_vars], interval=time_unit(freq), cache = TRUE)
x <- function_step(x, map_to_grid)
x <- function_step(x, n_obs_per_row)
x <- mutate_step(x, ~ . > 0)
x <- function_step(x, longest_rle, val = FALSE)
x <- filter_step(x, ~ . > as.numeric(ricu:::re_time(hours(12), time_unit(1)) / freq))
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
excl4 <- unique(x[, id_vars(x), with = FALSE])

print("apply exclusion critera: age")
# 5. Age < 18
x <- load_step("age")
x <- filter_step(x, ~ . < 18)

excl5 <- unique(x[, id_vars(x), with = FALSE])


print("apply exclusion critera: further exclusions")
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))

# Apply exclusions
patients <- exclude(patients, mget(paste0("excl", 1:5)))
attrition <- as.data.table(patients[c("incl_n", "excl_n_total", "excl_n")])
patients <- patients[['incl']]
patient_ids <- patients[, .SD, .SDcols = id_var(patients)]

print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
print("prepare data")
# Prepare data ------------------------------------------------------------

# Get predictors
# Write to disk -----------------------------------------------------------

out_path <- paste0(path, "/", src)

if (!dir.exists(out_path)) {
  dir.create(out_path, recursive = TRUE)
}

# save attrition
fwrite(attrition, paste0(out_path, "/attrition.csv"))
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
rm(attrition)
gc()
print(paste("Memory used after cleanup:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))

# save dynamic
dyn <- load_step(dict[dynamic_vars], interval=time_unit(freq), cache = TRUE)
dyn_fmt <- function_step(dyn, map_to_grid)
rename_cols(dyn_fmt, c("stay_id", "time"), meta_vars(dyn_fmt), by_ref = TRUE)
arrow::write_parquet(dyn_fmt, paste0(out_path, "/dyn.parquet"))

print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))
rm(dyn, dyn_fmt)
gc()
print(paste("Memory used after cleanup:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))

sta <- load_step(dict[static_vars], cache = TRUE)
sta_fmt <- sta[patient_ids]  # TODO: make into step
rename_cols(sta_fmt, c("stay_id"), id_vars(sta), by_ref = TRUE)
arrow::write_parquet(sta_fmt, paste0(out_path, "/sta.parquet"))

# Transform all variables into the target format
print(paste("Memory used:", round(as.numeric(mem_used()) / 1024^3, 3), "GB"))


