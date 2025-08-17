# [1] TIME SERIES MODEL: GENERAL AUTOREGRESSIVE MODEL (GAS)
# ROLLINGGAS IS OK
# GAS: generalised autoregressive score model: https://cran.r-project.org/web/packages/GAS/index.html
# comment out smaller_symbol_list to run on all the symbols
# Works now: using mirai package to speed up parallel processing through asynchronous programming
# 20% faster than mclapply

# @ outputs
# 1 is the first prediction
# subsample is the mean of the first half of the prediction (in the prediction window)
# all is the mean of all the predictions (in the prediction window)
# std means the standard deviation of all the predictions (in the prediction window)
# computes value at risk (VaR), expected shortfall (es), the 4 moments (mean, variance, skewness, kurtosis), location and scale

# @ description: uses the returns to compute VAR and ES

#======================================== Main Class ========================================
# create R6 class
feature_two <- R6::R6Class(
	"feature_two",
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS")
            for (package in required_packages){
                if (!require(package, character.only = TRUE, quietly = TRUE)){
                    message(paste0("Installing Missing Package: ", package))
                    install.packages(package, dependencies = TRUE)
                }
                suppressPackageStartupMessages(
                    library(package, character.only = TRUE, warn.conflicts = FALSE)
                )
            }
            message("The Required Packages Have Been Imported")
            options(warn = -1)
        }
    ),
	# initialize the class
	public = list(
		at = NULL, # row index which is used to calculate indicators
		windows = NULL, # rolling window length
		quantile_divergence_window = NULL, # window sizes for calculating rolling versions of the indicators
		num_cores = NULL,
		
		initialize = function(at = NULL, windows = c(5,22), quantile_divergence_window = c(50,100), num_cores = NULL){
            private$load_dependencies()
			self$at = at
			self$windows = windows
			self$quantile_divergence_window = quantile_divergence_window
			self$num_cores = parallel::detectCores()
		},
        #=========================================== FUNCTION TO LOAD DATA ===============================================
		load_data = function(current_path, database_name, table_name){
			# @description: load the data from duckdb
			# @params current_path: current path to the file
			# @params database_name: name of the database, basically the name of the db (without the db)
			# @params table_name: name of the table inside the database, use DBI::dbListTables() to see
			# @returns the daily historical data table for the entire US equity
			# checks
			checkmate::assert_character(current_path)
			checkmate::assert_character(database_name)
			checkmate::assert_character(table_name)
		
			tryCatch({
				database_directory <- paste0(current_path, "/" ,database_name, ".duckdb")
				con <- dbConnect(duckdb::duckdb(), dbdir = database_directory, read_only = FALSE)
				df <- as.data.table(dbReadTable(con, table_name))},
				error = function(e){
					new_current_path <- dirname(current_path) # move the path once
					database_directory <- paste0(new_current_path, "/" ,database_name, ".duckdb")
					con <- dbConnect(duckdb::duckdb(), dbdir = database_directory, read_only = FALSE)
					df <- as.data.table(dbReadTable(con, table_name))})
			# [first filter] symbols must have at least the size of the rolling windows
			windows_ <- c(5,10,22,22*3, 22*6,22*12,22*12*2)
			symbols_keep <- df[, .N, by = symbol][N > max(windows_), symbol]
			df <- df[symbol %in% symbols_keep]

            # set keys
            keycols = c("symbol", "date")
            setkeyv(df, keycols)

			return(df)},
        
        #======================================== Helper Function ========================================
        get_series_statistics = function(df, colname_prefix = "var") {
            id = value = col_name = `.` = variable = NULL

            stats <- lapply(df, function(x) {
            var_1 <- x[1] # first prediction
            var_subsample <- mean(x[1:floor(length(x)/2)], na.rm = TRUE) # mean of first half of the prediction period
            var_all <- mean(x, na.rm = TRUE) # mean of all periods,
            var_std <- sd(x, na.rm = TRUE) # standard deviation of all peridos
            list(var_1 = var_1, var_subsample = var_subsample, var_all = var_all, var_std = var_std)
            })

            stats <- melt(rbindlist(stats, idcol = "id"), id.vars = "id")
            stats[, col_name := paste(variable, gsub("\\.", "_", id), sep = "_")]
            stats <- transpose(stats[, .(col_name, value)], make.names = TRUE)
            colnames(stats) <- gsub("var", colname_prefix, colnames(stats))
            return(stats)
        },
        #=========================================== FUNCTION WITH THE MAIN ALGORITHM ===============================================
        rolling_function = function(x, window, price_col, params, get_series_statistics) {
            if (length(unique(x$symbol)) > 1) return(NA)

            # compute returns
            x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1]

            # initialise the GAS model
            GASSpec <- GAS::UniGASSpec(Dist = params$gas_dist,
                                        ScalingType = params$gas_scaling,
                                        GASPar = list(location = TRUE, scale = TRUE, shape = TRUE, skewness = TRUE))
                
            # fitting the GAS model
            Fit <- tryCatch(GAS::UniGASFit(GASSpec, na.omit(x$returns)), error = function(e) NA)
            if (!isS4(Fit)) return(NA)

            # forecasting using the GAS model
            # returns location(mean) and scale (standard deviation) WITH simulated draws (random values generated from model's forecasted distribution to represent possible future outcomes)
            y <- GAS::UniGASFor(Fit, H = params$prediction_horizont, ReturnDraws = TRUE)
            if (!isS4(y) || any(is.na(y@Draws))) return(NA)

            # calculates the value at risk thresholds based on simulated draws from model's predictive distribution
            q <- as.data.table(GAS::quantile(y, c(0.01, 0.05)))
            # computing the mean and standard deviation of returns...
            q <- get_series_statistics(q, "var")

            # calculates the expected shortfall thresholds based on simulated draws from model's predictive distribution
            es <- as.data.table(GAS::ES(y, c(0.01, 0.05)))
            es <- get_series_statistics(es, "es")

            # calculates the moments thresholds based on simulated draws from model's predictive distribution (the 4 moments: mean, variance, skewness, kurtosis)
            moments <- as.data.table(GAS::getMoments(y))
            moments <- get_series_statistics(moments, "moments")

            # get the parameter forecast: so the above: location, scale if any
            f <- as.data.table(GAS::getForecast(y))
            f <- get_series_statistics(f, "f")

            results <- cbind(q, es, moments, f)
            param_string <- paste0(unlist(params), collapse = "_")
            colnames(results) <- paste(colnames(results), window, param_string, sep = "_")
            return(results)
        },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, params, rolling_function, get_series_statistics){
            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional
            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            # bottleneck: mirai_map: works
            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col, params, get_series_statistics) {
                    data.table::setDTthreads(2)  # Allow light multithreading inside workers
                    out <- lapply(index_chunk, function(i) {
                        window_data <- dt[(i - windows_ + 1):i]
                        rolling_function(window_data, windows_, price_col, params, get_series_statistics)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(dt = subset_df,
                            windows_ = windows_,
                            price_col = price_col,
                            params = params,
                            rolling_function = rolling_function,
                            get_series_statistics = get_series_statistics)
            )[]


            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("GAS_", stat_names[j], "_", windows_)
                result_dt[indices, (colname) := res_matrix[[j]]]
            }

            result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
            new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
            subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
            return(subset_df)
            },
        
        other_finfeatures_indicators = function(symbol_df) {
            ohlcv <- data.table::copy(symbol_df)
            data.table::setkey(ohlcv, symbol, date) # added this in 020825
            data.table::setDTthreads(threads = 16, throttle = 1)
            setorder(ohlcv, symbol, date)
            
            print("=================== COMPUTING OTHER INDICATORS FROM FINFEATURES NOW ===================")
            #=================== OTHER INDICATORS 4: Generalised Autoregressive Score (GAS) Model ===================
            # decided not to use more than 1 type of distribution and scaling: for every 1 permutation, 40 new features are created
            # creating a forecasting model using GAS
            print("[4] Calculating Other Indicators Generalised Autoregressive Score (GAS) Model...")
            start_time <- tictoc::tic()
            #=========================================== MIRAI function (parallel processing) ===============================================
            # set up mirai for parallel processing
            if (mirai::daemons()$connections == 0){
                mirai::daemons(16) # force the daemons
            }

            mirai::everywhere({
                library(data.table)
                })
            
            # normal set up
            windows_ <- 252
            price_col <- "close"
            params <- list(gas_dist = "norm", gas_scaling = "Identity", prediction_horizont = 10)
            # ======================================= the code to be runned =======================================
            results_dt <- as.data.table(self$run_code(symbol_df, windows_, price_col, params, self$rolling_function, self$get_series_statistics))
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
			gas_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Generalised Autoregressive Score (GAS) ", gas_time, " Seconds"))
            rm(start_time, windows_, price_col, params, results_dt, new_cols, end_time, gas_time)
            mirai::daemons(0) # stop the active daemon process cleanly and clean up background workers
            # return(res_list)s
            data.table::setkey(ohlcv, symbol, date)
            return(ohlcv) # might be return(ohlcv)
        }
    )
)

# ================================================================ END OF CODE, TEST NOW ================================================================
# feature_loader <- feature_two$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")
# aapl_df <- df[symbol=="aapl"]
# prepare_df <- feature_loader$other_finfeatures_indicators(aapl_df) #test2: 94 seconds to complete AAPL

# to check
# prepare_df["a"] # check
# prepare_df["aa"] # check
# colnames(prepare_df$a)
# ================================================================ END OF CODE, TEST NOW ================================================================
# functions to be used
# get_series_statistics = function(df, colname_prefix = "var") {
#     id = value = col_name = `.` = variable = NULL

#     stats <- lapply(df, function(x) {
#     var_1 <- x[1] # first prediction
#     var_subsample <- mean(x[1:floor(length(x)/2)], na.rm = TRUE) # mean of first half of the prediction period
#     var_all <- mean(x, na.rm = TRUE) # mean of all periods,
#     var_std <- sd(x, na.rm = TRUE) # standard deviation of all peridos
#     list(var_1 = var_1, var_subsample = var_subsample, var_all = var_all, var_std = var_std)
#     })

#     stats <- melt(rbindlist(stats, idcol = "id"), id.vars = "id")
#     stats[, col_name := paste(variable, gsub("\\.", "_", id), sep = "_")]
#     stats <- transpose(stats[, .(col_name, value)], make.names = TRUE)
#     colnames(stats) <- gsub("var", colname_prefix, colnames(stats))
#     return(stats)
# }

# rolling_function = function(x, window, price_col, params, get_series_statistics) {
#     if (length(unique(x$symbol)) > 1) return(NA)

#     # compute returns
#     x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1]

#     # initialise the GAS model
#     GASSpec <- GAS::UniGASSpec(Dist = params$gas_dist,
#                                 ScalingType = params$gas_scaling,
#                                 GASPar = list(location = TRUE, scale = TRUE, shape = TRUE, skewness = TRUE))
        
#     # fitting the GAS model
#     Fit <- tryCatch(GAS::UniGASFit(GASSpec, na.omit(x$returns)), error = function(e) NA)
#     if (!isS4(Fit)) return(NA)

#     # forecasting using the GAS model
#     # returns location(mean) and scale (standard deviation) WITH simulated draws (random values generated from model's forecasted distribution to represent possible future outcomes)
#     y <- GAS::UniGASFor(Fit, H = params$prediction_horizont, ReturnDraws = TRUE)
#     if (!isS4(y) || any(is.na(y@Draws))) return(NA)

#     # calculates the value at risk thresholds based on simulated draws from model's predictive distribution
#     q <- as.data.table(GAS::quantile(y, c(0.01, 0.05)))
#     # computing the mean and standard deviation of returns...
#     q <- get_series_statistics(q, "var")

#     # calculates the expected shortfall thresholds based on simulated draws from model's predictive distribution
#     es <- as.data.table(GAS::ES(y, c(0.01, 0.05)))
#     es <- get_series_statistics(es, "es")

#     # calculates the moments thresholds based on simulated draws from model's predictive distribution (the 4 moments: mean, variance, skewness, kurtosis)
#     moments <- as.data.table(GAS::getMoments(y))
#     moments <- get_series_statistics(moments, "moments")

#     # get the parameter forecast: so the above: location, scale, shape, skewness, if any
#     f <- as.data.table(GAS::getForecast(y))
#     f <- get_series_statistics(f, "f")

#     results <- cbind(q, es, moments, f)
#     param_string <- paste0(unlist(params), collapse = "_")
#     colnames(results) <- paste(colnames(results), window, param_string, sep = "_")
#     return(results)
# }

# # mirai::daemons()$connections # check number of daemons currently running 
# # mirai::daemons() # full status
# # IMPORTANT CODE
# if (mirai::daemons()$connections == 0){
#     mirai::daemons(parallel::detectCores())
# }

# mirai::everywhere(library(data.table))

# symbol_list <- split(df, by = "symbol", keep.by = TRUE)

# #====================================================== set up ======================================================
# # aapl_df <- df["aapl"]
# aapl_df <- symbol_list[[1]]
# windows_ <- 252 # window
# price_col <- "close" # price column
# params <- list(gas_dist = "norm", gas_scaling = "Identity", prediction_horizont = 10) # params

# #====================================================== set up 2 ======================================================
# n_rows <- nrow(aapl_df)
# indices <- (windows_ + 1):n_rows # from second row onwards because first row's return is NA
# result_dt <- data.table(date = aapl_df$date)

# #====================================================== mirai function ======================================================
# mirai_function = function(i, dt, windows_, price_col, params, rolling_function, get_series_statistics){
#     window_data <- dt[(i - windows_ + 1):i]
#     rolling_function(window_data, windows_, price_col = price_col, params = params, get_series_statistics)
# }

# # Parallel computation: run rolling_function over each sliding window # 77 seconds (faster than mclapply): about 20% more efficient
# results <- mirai::mirai_map(
#     .x = indices,
#     .f = mirai_function,
#     .args = list(dt = aapl_df,
#                 windows_ = 252,
#                 price_col = "close",
#                 params = list(gas_dist = "norm", gas_scaling = "Identity", prediction_horizont = 10),
#                 rolling_function = rolling_function,
#                 get_series_statistics = get_series_statistics)
# )[]

# res_matrix <- do.call(rbind, results)
# stat_names <- colnames(res_matrix)

# for (j in seq_along(stat_names)) {
#     colname <- paste0("GAS_", stat_names[j], "_", windows_)
#     result_dt[indices, (colname) := res_matrix[[j]]]
# }

# result_dt[, symbol := aapl_df2$symbol[1]]  # retain symbol

# result_dt

# #======================================== BACK UP IF ALL GOES TO SHIT ==================================================
# #======================================== Helper Function ========================================
# get_series_statistics = function(df, colname_prefix = "var") {
#     id = value = col_name = `.` = variable = NULL

#     stats <- lapply(df, function(x) {
#     var_1 <- x[1] # first prediction
#     var_subsample <- mean(x[1:floor(length(x)/2)], na.rm = TRUE) # mean of first half of the prediction period
#     var_all <- mean(x, na.rm = TRUE) # mean of all periods,
#     var_std <- sd(x, na.rm = TRUE) # standard deviation of all peridos
#     list(var_1 = var_1, var_subsample = var_subsample, var_all = var_all, var_std = var_std)
#     })

#     stats <- melt(rbindlist(stats, idcol = "id"), id.vars = "id")
#     stats[, col_name := paste(variable, gsub("\\.", "_", id), sep = "_")]
#     stats <- transpose(stats[, .(col_name, value)], make.names = TRUE)
#     colnames(stats) <- gsub("var", colname_prefix, colnames(stats))
#     return(stats)
# }
# #======================================== Main Function ========================================
# rolling_function = function(x, window, price_col, params, get_series_statistics) {
#     if (length(unique(x$symbol)) > 1) return(NA)

#     # compute returns
#     x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1]

#     # initialise the GAS model
#     GASSpec <- GAS::UniGASSpec(Dist = params$gas_dist,
#                                 ScalingType = params$gas_scaling,
#                                 GASPar = list(location = TRUE, scale = TRUE, shape = TRUE, skewness = TRUE))
        
#     # fitting the GAS model
#     Fit <- tryCatch(GAS::UniGASFit(GASSpec, na.omit(x$returns)), error = function(e) NA)
#     if (!isS4(Fit)) return(NA)

#     # forecasting using the GAS model
#     # returns location(mean) and scale (standard deviation) WITH simulated draws (random values generated from model's forecasted distribution to represent possible future outcomes)
#     y <- GAS::UniGASFor(Fit, H = params$prediction_horizont, ReturnDraws = TRUE)
#     if (!isS4(y) || any(is.na(y@Draws))) return(NA)

#     # calculates the value at risk thresholds based on simulated draws from model's predictive distribution
#     q <- as.data.table(GAS::quantile(y, c(0.01, 0.05)))
#     # computing the mean and standard deviation of returns...
#     q <- get_series_statistics(q, "var")

#     # calculates the expected shortfall thresholds based on simulated draws from model's predictive distribution
#     es <- as.data.table(GAS::ES(y, c(0.01, 0.05)))
#     es <- get_series_statistics(es, "es")

#     # calculates the moments thresholds based on simulated draws from model's predictive distribution (the 4 moments: mean, variance, skewness, kurtosis)
#     moments <- as.data.table(GAS::getMoments(y))
#     moments <- get_series_statistics(moments, "moments")

#     # get the parameter forecast: so the above: location, scale, shape, skewness, if any
#     f <- as.data.table(GAS::getForecast(y))
#     f <- get_series_statistics(f, "f")

#     results <- cbind(q, es, moments, f)
#     param_string <- paste0(unlist(params), collapse = "_")
#     colnames(results) <- paste(colnames(results), window, param_string, sep = "_")
#     return(results)
# }

# mirai_function = function(i, dt, windows_, price_col, params, rolling_function, get_series_statistics){
#     data.table::setDTthreads(1)
#     window_data <- dt[(i - windows_ + 1):i]
#     rolling_function(window_data, windows_, price_col = price_col, params = params, get_series_statistics)
# }

# run_code = function(subset_df, windows_, price_col, params, rolling_function, get_series_statistics){
#     start_time <- tictoc::tic()
#     n_rows <- nrow(subset_df)
#     indices <- (windows_ + 1):n_rows

#     result_dt <- data.table(date = subset_df$date)


#     # bottleneck: mirai_map: works
#     results <- mirai::mirai_map(
#         .x = indices,
#         .f = mirai_function,
#         .args = list(dt = subset_df,
#                     windows_ = windows_,
#                     price_col = price_col,
#                     params = params,
#                     rolling_function = rolling_function,
#                     get_series_statistics = get_series_statistics)
#     )[]


#     res_matrix <- do.call(rbind, results)
#     stat_names <- colnames(res_matrix)

#     for (j in seq_along(stat_names)) {
#         colname <- paste0("GAS_", stat_names[j], "_", windows_)
#         result_dt[indices, (colname) := res_matrix[[j]]]
#     }

#     result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
#     new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
#     subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]

#     end_time <- tictoc::toc()
#     gas_time <- round(end_time$toc - end_time$tic,3)
#     print(paste0("Total Time Taken to Compute Generalised Autoregressive Score (GAS) for this symbol is ", gas_time, " Seconds"))
#     return(subset_df)
#     }

# #======================================== Main Class ========================================
# # create R6 class
# ohlcv_features_basic <- R6::R6Class(
# 	"ohlcv_features_basic",
# 	# initialize the class
# 	public = list(
# 		at = NULL, # row index which is used to calculate indicators
# 		windows = NULL, # rolling window length
# 		quantile_divergence_window = NULL, # window sizes for calculating rolling versions of the indicators
# 		num_cores = NULL,
		
# 		initialize = function(at = NULL, windows = c(5,22), quantile_divergence_window = c(50,100), num_cores = NULL){
# 			self$at = at
# 			self$windows = windows
# 			self$quantile_divergence_window = quantile_divergence_window
# 			self$num_cores = parallel::detectCores()
# 		},
# 		#=== FIRST FUNCTION ===
# 		load_data = function(current_path, database_name, table_name){
# 			# @description: load the data from duckdb
# 			# @params current_path: current path to the file
# 			# @params database_name: name of the database, basically the name of the db (without the db)
# 			# @params table_name: name of the table inside the database, use DBI::dbListTables() to see
# 			# @returns the daily historical data table for the entire US equity
# 			# checks
# 			checkmate::assert_character(current_path)
# 			checkmate::assert_character(database_name)
# 			checkmate::assert_character(table_name)
		
# 			tryCatch({
# 				database_directory <- paste0(current_path, "/" ,database_name, ".duckdb")
# 				con <- dbConnect(duckdb::duckdb(), dbdir = database_directory, read_only = FALSE)
# 				df <- as.data.table(dbReadTable(con, table_name))},
# 				error = function(e){
# 					new_current_path <- dirname(current_path) # move the path once
# 					database_directory <- paste0(new_current_path, "/" ,database_name, ".duckdb")
# 					con <- dbConnect(duckdb::duckdb(), dbdir = database_directory, read_only = FALSE)
# 					df <- as.data.table(dbReadTable(con, table_name))})
# 			# [first filter] symbols must have at least the size of the rolling windows
# 			windows_ <- c(5,10,22,22*3, 22*6,22*12,22*12*2)
# 			symbols_keep <- df[, .N, by = symbol][N > max(windows_), symbol]
# 			df <- df[symbol %in% symbols_keep]

#             # set keys
#             keycols = c("symbol", "date")
#             setkeyv(df, keycols)

# 			return(df)},
        
#         other_finfeatures_indicators = function(df) {
#             ohlcv <- data.table::copy(df)
#             data.table::setDTthreads(threads = 0, throttle = 1)
#             setorder(ohlcv, symbol, date)
            
#             print("=================== COMPUTING OTHER INDICATORS FROM FINFEATURES NOW ===================")
#             Sys.sleep(2)
#             #=================== OTHER INDICATORS 4: Generalised Autoregressive Score (GAS) Model ===================
#             # decided not to use more than 1 type of distribution and scaling: for every 1 permutation, 40 new features are created
#             # creating a forecasting model using GAS
#             print("[4] Calculating Other Indicators Generalised Autoregressive Score (GAS) Model...")
#             start_time <- tictoc::tic()
#             gas_dt <- data.table::copy(ohlcv)
#             #=========================================== MIRAI function (parallel processing) ===============================================
#             # set up mirai for parallel processing
#             if (mirai::daemons()$connections == 0){
#                 mirai::daemons(parallel::detectCores())
#             }

#             mirai::everywhere({
#                 library(data.table)
#                 })
            
#             # normal set up
#             windows_ <- 252
#             price_col <- "close"
#             params <- list(gas_dist = "norm", gas_scaling = "Identity", prediction_horizont = 10)
#             # ======================================= the code to be runned =======================================
#             ### Splitting the tickers
#             symbol_list <- split(gas_dt, by = "symbol", keep.by = TRUE)
#             smaller_symbol_list <- symbol_list[1:2] # try on 2 symbols first

#             try_list <- lapply(smaller_symbol_list, function(dt) {
#                 run_code(dt, windows_, price_col, params, rolling_function, get_series_statistics)})

#             results_dt <- rbindlist(try_list, use.names = TRUE, fill = TRUE)
#             setkey(results_dt, symbol, date)
#             new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
#             gas_dt[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

#             end_time <- tictoc::toc()
# 			gas_time <- round(end_time$toc - end_time$tic,3)
#             print(paste0("Total Time Taken to Compute Generalised Autoregressive Score (GAS) ", gas_time, " Seconds"))
#             # return(res_list)s
#             return(try_list)
#         }
#     )
# )