#' @description feature engineering process for all the symbols
#' @note uses run_features_set_1() for features related to prices and time series
#' @note uses run_features_set_2() for features related to fundamentals, macros and prices
#' @update 290725 updated the run_features_set_1() algorithm to SAVE all batches every time it finishes running: in case there is an error, at least some of the work will be saved
consolidated_features <- R6::R6Class(
    "consolidated_features",
    private = list(
        load_sources = function(){
            setwd("~") # use this to reset the working directory
            current_path <- getwd() # "/Users/arthurgoh"
            path_to_files <- "/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/3_preprocess_data"
            path_to_data <- "/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate"
            file_path <- paste0(current_path, path_to_files)

            source(paste0(file_path, "/3_preprocess_class_draft2.R"))
            source(paste0(file_path, "/3_preprocess_class_I.R"))
            source(paste0(file_path, "/3_preprocess_class_II.R"))
            source(paste0(file_path, "/3_preprocess_class_III.R"))
            source(paste0(file_path, "/3_preprocess_class_IV.R"))
            source(paste0(file_path, "/3_preprocess_class_V.R"))
            source(paste0(file_path, "/3_preprocess_class_VI.R"))
            source(paste0(file_path, "/3_preprocess_class_VII.R"))
            source(paste0(file_path, "/3_preprocess_class_VIII.R"))
            source(paste0(file_path, "/3_preprocess_class_IX.R"))
            source(paste0(file_path, "/3_preprocess_class_X.R"))
            source(paste0(file_path, "/3_preprocess_class_XI.R"))
            
            source(paste0(file_path, "/tables.R"))
            source(paste0(file_path, "/utils.R"))
            source(paste0(file_path, "/extract_data_fmp_class_copy.R"))
            source(paste0(file_path, "/factors_copy.R"))
            source(paste0(file_path, "/import_copy.R"))
        }
    ),

    public = list(
        current_path = NULL,
        path_to_files = NULL,
        path_to_data = NULL,
        file_path = NULL,
        technical_indicators = NULL,
        feature_one = NULL,
        feature_two = NULL,
        feature_three = NULL,
        feature_four = NULL,
        feature_five = NULL,
        feature_six = NULL,
        feature_seven = NULL,
        feature_eight = NULL,
        feature_nine = NULL,
        feature_ten = NULL,
        feature_eleven = NULL,
        fmp = NULL,
        import_data = NULL,
        factors = NULL,

        initialize = function(){
            self$current_path <- getwd() # "/Users/arthurgoh"
            self$path_to_files <- "/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/3_preprocess_data"
            self$path_to_data <- "/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate"
            self$file_path <- paste0(self$current_path, self$path_to_files)

            private$load_sources()


            # initiate the different classes
            self$technical_indicators <- technical_indicators$new()
            self$feature_one <- feature_one$new()
            self$feature_two <- feature_two$new()
            self$feature_three <- feature_three$new()
            self$feature_four <- feature_four$new()
            self$feature_five <- feature_five$new()
            self$feature_six <- feature_six$new()
            self$feature_seven <- feature_seven$new()
            self$feature_eight <- feature_eight$new()
            self$feature_nine <- feature_nine$new()
            self$feature_ten <- feature_ten$new()
            self$feature_eleven <- feature_eleven$new()

            self$fmp <- extract_data_FMP$new()
            self$import_data <- data_related_to_stock$new()
            self$factors <- get_factors$new()
        },
        get_symbols = function(index = NULL){
            #' @description get the symbols for a particular index: sp500/nasdaq/dowjones
            res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/', index, '-constituent?apikey=u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH'))
            res_content <- content(res, as = "text", encoding = "UTF-8")
            index_companies <- fromJSON(res_content) %>% as.data.table()
            symbols <- index_companies$symbol
            return(symbols)
        },
        load_data = function(){
            #' @description retrieve ALL data in the index
            #' @return OHLCV data, with upper casing symbols and date
            con <- dbConnect(duckdb::duckdb(), dbdir = paste0(self$current_path, self$path_to_data, "/stocks_daily.duckdb"), read_only = FALSE)
            df <- as.data.table(dbReadTable(con, "stocks_daily"))
            df[, symbol := toupper(symbol)]
            return(df)
        },
        prepare_features_pipeline = function(symbol_df){
            #' @description the entire pipeline to prepare features
            #' @description encompass everything: basically a workflow: for time series features (under finfeatures package for Mr Mislav)
            #' @note works on a per symbol basis: cannot use parallel computing here because within each function here, parallel programming is being utilised
            #' @note used max_duration because some features takes too long to run (quarks for CTRA), skip that feature. when combined, introduce NAs: WORKS
            
            max_duration = 1800 # 30 minutes
            message(paste0("========================================================== Creating Features for: ", unique(symbol_df$symbol), " now =========================================================="))
            ohlcv = data.table::copy(symbol_df)
            # ohlcv1 = self$technical_indicators$prepare_data(ohlcv)
            # ohlcv2 = self$technical_indicators$prepare_technical_indicators(ohlcv1)
            # ohlcv3 = self$feature_one$other_finfeatures_indicators(ohlcv2)
            # ohlcv4 = self$feature_two$other_finfeatures_indicators(ohlcv3)
            # ohlcv5 = self$feature_three$other_finfeatures_indicators(ohlcv4)
            # ohlcv6 = self$feature_four$other_finfeatures_indicators(ohlcv5)
            # ohlcv7 = self$feature_five$other_finfeatures_indicators(ohlcv6)
            # ohlcv8 = self$feature_six$other_finfeatures_indicators(ohlcv7)
            # ohlcv9 = self$feature_seven$other_finfeatures_indicators(ohlcv8)
            # ohlcv10 = self$feature_eight$other_finfeatures_indicators(ohlcv9)
            # ohlcv11 = self$feature_nine$other_finfeatures_indicators(ohlcv10)
            # ohlcv12 = self$feature_ten$other_finfeatures_indicators(ohlcv11)
            # ohlcv13 = self$feature_eleven$other_finfeatures_indicators(ohlcv12)
            # return(ohlcv13)

            run_step <- function(expr, step_name) {
                #' @description withTimeout() monitors the execution time of the code
                message(sprintf("[%s] starting …", step_name))
                out <- tryCatch(
                    withTimeout(eval(expr), timeout = max_duration, onTimeout = "error"),
                    TimeoutException = function(e) {
                        message(sprintf("[%s] skipped – 30 min limit reached", step_name))
                        NULL
                    }, error = function(e) {
                        message(sprintf("[%s] failed: %s", step_name, e$message))
                        NULL
                    })
                out
            }

            steps <- list(
                ohlcv_base = quote(self$technical_indicators$prepare_data(ohlcv)),
                ohlcv_ta = quote(self$technical_indicators$prepare_technical_indicators(ohlcv)),
                feature_1 = quote(self$feature_one$other_finfeatures_indicators(ohlcv)),
                feature_2 = quote(self$feature_two$other_finfeatures_indicators(ohlcv)),
                feature_3 = quote(self$feature_three$other_finfeatures_indicators(ohlcv)),
                feature_4 = quote(self$feature_four$other_finfeatures_indicators(ohlcv)),
                feature_5 = quote(self$feature_five$other_finfeatures_indicators(ohlcv)),
                feature_6 = quote(self$feature_six$other_finfeatures_indicators(ohlcv)),
                feature_7 = quote(self$feature_seven$other_finfeatures_indicators(ohlcv)),
                feature_8 = quote(self$feature_eight$other_finfeatures_indicators(ohlcv)),
                feature_9 = quote(self$feature_nine$other_finfeatures_indicators(ohlcv)),
                feature_10 = quote(self$feature_ten$other_finfeatures_indicators(ohlcv)),
                feature_11 = quote(self$feature_eleven$other_finfeatures_indicators(ohlcv))
                )
            
            feature_list <- lapply(names(steps), \(n) run_step(steps[[n]], n))
            feature_list <- Filter(Negate(is.null), feature_list)
            lapply(feature_list, setkey, symbol, date)
            final_dt <- Reduce(
                f = function(x, y) {
                    cols_to_keep <- c("symbol", "date", setdiff(names(y), names(x)))
                    y_new <- y[, ..cols_to_keep]
                    merge(x, y_new, by = c("symbol", "date"), all = TRUE)
                }, x = feature_list)
            
            return(final_dt)
        },
        partial_run_features_set_1 = function(start_index, end_index, index_data, symbols){
            #' @description selects batches of symbols to run at one go
            #' @note tries to solve the issue of throttle in the CPU
            #' @param start_index and end_index is used to choose the position for the index symbols
            #' @param index_data is the index data extracted from the database
            #' @param symbols refers to the symbols in the index
            
            filtered_symbols <- symbols[start_index:end_index]
            filtered_data <- index_data[symbol %in% filtered_symbols]
            total_symbols <- length(symbols) # count ALL the symbols
            current_index <- start_index # tracks overall position
            
            features_list <- list()
            for (targeted_symbol in filtered_symbols) {
                message(paste0("========================================================== DOING ", current_index, "/", total_symbols, " NOW =========================================================="))
               symbol_data <- filtered_data[symbol == targeted_symbol]
            #    self$reset_openmp() # try
               features_dt <- self$prepare_features_pipeline(symbol_data)
               features_list[[targeted_symbol]] <- features_dt
               current_index <- current_index + 1 # advance the overall position
            }
            final_features_dt <- data.table::rbindlist(features_list, use.names = TRUE, fill = TRUE)
            return(final_features_dt)
        },
        run_features_set_1 = function(){
            sp500_symbols <- self$get_symbols("sp500")
            sp500_symbols <- toupper(gsub("[^A-Za-z]", "", sp500_symbols)) # ensures ONLY all characters, and in upper casing
            sp500_data <- self$load_data() # data is being collected

            consolidated_dt_path <- file.path(self$file_path, "consolidated_data/consolidated_dt.duckdb")

            # if (file.exists(consolidated_dt_path)) unlink(consolidated_dt_path) # creates an empty duckdb again
            con <- dbConnect(duckdb::duckdb(), dbdir = consolidated_dt_path, read_only = FALSE)
            # on.exit(dbDisconnect(con, shutdown = TRUE))
            existing_tables <- DBI::dbListTables(con)
            
            #==================================== Workflow Start ====================================
            index_data <- sp500_data[symbol %in% sp500_symbols]
            num_symbols <- length(sp500_symbols) # USE THIS USUALLY

            processed_batches <- as.integer(gsub("batch_", "", existing_tables[grepl("^batch_", existing_tables)]))
            start_batch <- max(processed_batches, na.rm = TRUE) + 1
            message(paste0("start batch is: ", start_batch))
            
            #' @note split into batches of 5 symbols and save them into a database in duckdb
            #' @note learnt from previous mistake of trying to run everything first before saving, bad idea because it takes one error to ruin all the progress
            #' @note and saves us the trouble of worrying about memory
            
            all_dt <- data.table() # to save all the batches of symbols together
            # batches <- split(seq_along(sp500_symbols), ceiling(seq_along(sp500_symbols) / 1)) # TEST
            batches <- split(seq_along(sp500_symbols), ceiling(seq_along(sp500_symbols) / 5))

            # batch_res <- lapply(seq_along(batches), function(i) {
            batch_res <- lapply(start_batch:length(batches), function(i) {
                idx_vec <- batches[[i]]

                res <- self$partial_run_features_set_1(idx_vec[1], idx_vec[length(idx_vec)], index_data, sp500_symbols)
                all_dt <<- rbindlist(list(all_dt, res), use.names = TRUE, fill = TRUE)
                tbl_name <- sprintf("batch_%03d", i) # save the batch as its own data table: to start MLR process
                DBI::dbWriteTable(con, tbl_name, res, overwrite = TRUE)
                message(sprintf("Batch %d/%d written (%s rows)", i, length(batches), format(nrow(res), big.mark = ",")))
                message(base::strrep("=", 120))
                })

            # consolidated_dt <- data.table::rbindlist(mget(paste0("batch_", 1:102)), use.names = TRUE, fill = TRUE)
            # DBI::dbWriteTable(con, "0_consolidated_dt", all_dt, overwrite = TRUE) # the consolidate data table
            all_batches <- DBI::dbListTables(con)[grepl("^batch_", DBI::dbListTables(con))]
            combined <- rbindlist(lapply(all_batches, function(tbl) DBI::dbReadTable(con, tbl)), use.names = TRUE, fill = TRUE)
            DBI::dbWriteTable(con, "0_consolidated_dt", combined, overwrite = TRUE)
            return(all_dt)
        },
        run_features_set_2 = function(){
            #' @description encompasses everything: basically a workflow: for macros (under findata package for the factors.R for Mr Mislav)
            #' @return price, fundamentals and macros data for sp500 symbols: use the same get_index_companies("sp500") function
            factors_data <- self$factors$get_factor_data()
            return(factors_data)
        }
    )
)


# now
a <- consolidated_features$new()
df1 <- a$run_features_set_1() # main thing to run

# symbols <- a$get_symbols(index = "sp500")
# ok_symbol <- symbols[252]
# problem_symbol <- symbols[253]

# all_data <- a$load_data()
# ok_symbol_data <- all_data[symbol == ok_symbol]

# problem_symbol_data <- all_data[symbol == problem_symbol]


# run_ok_symbol <- a$prepare_features_pipeline(ok_symbol_data)
# run_problem_symbol <- a$prepare_features_pipeline(problem_symbol_data)

# feature_list <- list()
# feature_list[[ok_symbol]] <- run_ok_symbol
# feature_list[[problem_symbol]] <- run_problem_symbol
# final_features_dt <- data.table::rbindlist(feature_list, use.names = TRUE, fill = TRUE)

# special cases: brk-b, bf-b

# sp500_data <- a$load_data()
# symbols <- a$get_symbols("sp500")
# now_data <- sp500_data[symbol == "BFB"] # load data using this
# test1 <- a$prepare_features_pipeline(erie_data) # test the pipeline
# view(test1)

# view(df1)
# unique(df1$symbol)

# symbols[226]
# # ceiling(length(symbols)/10)

# str(erie_data)
# str(sp500_data[symbol == "DELL"])
# index_data <- sp500_data[symbol %in% symbols]

# batches[[length(batches)]]
# length(batches)
# min(batches[[2]])
# max(batches[[2]])

# length(symbols)
# symbols[1:5]
# symbols[6:10]


# batches[110]


# view(symbols)

# df <- a$load_data()
# view(df[symbol == "GEV"])

# sp500_df <- df[symbol %in% symbols]
# sp500_df[, .N, by = symbol][N < 252]
# sp500_df[, .N, by = symbol][order(N)][1]
#======= START =======
# sp500 <- fmp$get_index_companies("sp500")
# sp500_symbols <- sp500$symbol
# require(httr)
# res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/sp500-constituent','?apikey=', "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"))
# res_content <- content(res, as = "text", encoding = "UTF-8")
# index_companies <- fromJSON(res_content) %>% as.data.table()

# # load data
# con <- dbConnect(duckdb::duckdb(), dbdir = paste0(current_path, path_to_data, "/stocks_daily.duckdb"), read_only = FALSE)
# df <- as.data.table(dbReadTable(con, "stocks_daily"))
# df[, symbol := toupper(symbol)]
# df <- df[symbol %in% sp500_symbols] # filter out only sp500 symbols

# # main function to run: CANNOT USE PARALLEL FUNCTIONS ALREADY BECAUSE IT IS INSIDE
# run_code = function(symbol_df){
#     ohlcv = data.table::copy(symbol_df)
#     ohlcv1 = technical_indicators$prepare_data(ohlcv)
#     ohlcv2 = technical_indicators$prepare_technical_indicators(ohlcv1)
#     ohlcv3 = feature_one$other_finfeatures_indicators(ohlcv2)
#     ohlcv4 = feature_two$other_finfeatures_indicators(ohlcv3)
#     ohlcv5 = feature_three$other_finfeatures_indicators(ohlcv4)
#     ohlcv6 = feature_four$other_finfeatures_indicators(ohlcv5)
#     ohlcv7 = feature_five$other_finfeatures_indicators(ohlcv6)
#     ohlcv8 = feature_six$other_finfeatures_indicators(ohlcv7)
#     ohlcv9 = feature_seven$other_finfeatures_indicators(ohlcv8)
#     ohlcv10 = feature_eight$other_finfeatures_indicators(ohlcv9)
#     ohlcv11 = feature_nine$other_finfeatures_indicators(ohlcv10)
#     ohlcv12 = feature_ten$other_finfeatures_indicators(ohlcv11)
#     return(ohlcv3)
# }

# # workflow
# first_symbol = sp500_symbols[1]
# first_df <- df[symbol == first_symbol]
# first_symbol_features <- run_code(first_df)
# view(first_symbol_features)


# factors_data <- factors$get_factor_data() # returns price, fundamentals and macros data for sp500 symbols: use the same get_index_companies("sp500") function

# view(factors_data$prices_factors) # too big: dont run: WORKS
# view(factors_data$fundamental_factors) # WORKS
# view(factors_data$macro) # WORKS
