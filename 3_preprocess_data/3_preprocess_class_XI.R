# 12: ROLLINGWAVELETARIMA
#' @link https://cran.r-project.org/web/packages/WaveletArima/WaveletArima.pdf
#' @link https://cran.r-project.org/web/packages/WaveletArima/index.html
#' @description Noise in the time-series data significantly affects the accuracy of the ARIMA model. 
#' Wavelet transformation decomposes the time series data into subcomponents to reduce the noise and help to improve the model performance. 
#' The waveletARIMA model can achieve higher prediction accuracy than the traditional ARIMA model. 

#======= Main Class =======
# create R6 class
feature_eleven <- R6::R6Class(
	"feature_eleven",
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch", "quarks", "ufRisk", "theftdlc", "theft", "tsfeatures", "tsDyn", "fracdiff", "forecast", "WaveletArima", "backCUSUM")
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
    #=========================================== INITIALISING THE CLASS ===============================================
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
        #=========================================== FUNCTION WITH THE MAIN ALGORITHM ===============================================
        rolling_function = function(x, window, price_col, params){
        # @description: the main algorithm for TsDyn
        # @params: x is the dataframe, containing date, symbol and ohlcv
        # @params: window: size of the rolling window
        # @params: price_col: the price column to be used: OHLCV
        # @params: params: these are EXPANDED GRIDS assigned as global variable to improve efficiency in the code
        # @returns: values given by that time series method
            # main algorithm, dependent on method
            
            # compute returns
            x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1]

            output_cols <- list()  # collect all column values
            # calculate arima forecasts
            for (i in seq_along(nrow(params))){
                filter = params$filter[i]

                y <- WaveletArima::WaveletFittingarma(ts = na.omit(x$returns),
                    filter = filter,
                    Waveletlevels = floor(log(length(x$returns))),
                    MaxARParam = params$MaxARParam[i],
                    MaxMAParam = params$MaxMAParam[i],
                    NForecast = params$NForecast[i])


                # forecasts <- data.table::as.data.table(t(y$Finalforecast))
                # colnames(forecasts) <- paste0("WaveletFittingarma_forecasts_", seq_along(forecasts), "_", window)
                
                mean_val  <- mean(y$Finalforecast)
                sd_val    <- sd(y$Finalforecast)
                output_cols[[i]] <- c(
                    setNames(mean_val, paste0("WaveletFittingarma_forecasts_mean_", window)),
                    setNames(sd_val, paste0("WaveletFittingarma_forecasts_sd_", window))
                )
            }
            output <- as.data.table(as.list(unlist(output_cols)))
            return(output)
        },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, rolling_function, params){
            # @description: uses the mirai package to integrate asynchronous parallel programming into the script

            # subset_df[, returns := get(price_col) / data.table::shift(get(price_col)) - 1][!is.na(returns)] # compute returns, removes first row

            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional

            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col, params) {
                    data.table::setDTthreads(2)  # Allow light multithreading inside workers
                    out <- lapply(index_chunk, function(i) {
                        window_data <- dt[(i - windows_ + 1):i]
                        rolling_function(window_data, windows_, price_col, params)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(
                    dt = subset_df,
                    windows_ = windows_,
                    rolling_function = rolling_function,
                    price_col = price_col,
                    params = params
                )
            )[]

            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("waveletarima_", stat_names[j])
                result_dt[indices, (colname) := res_matrix[[j]]]
            }

            result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
            new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
            subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
            return(subset_df)
        },
        #=========================================== CONSOLIDATION CODE ===============================================
        other_finfeatures_indicators = function(symbol_df) {
            # @description: consolidating all functions together to run as 1 function
            ohlcv <- data.table::copy(symbol_df)
            data.table::setkey(ohlcv, symbol, date) # added this in 020825
            data.table::setDTthreads(threads = 16, throttle = 1)
            setorder(ohlcv, symbol, date)
            
            print("=================== [13] COMPUTING time series features from waveletarima Package ===================")
            #=================== OTHER INDICATORS 13: forecasted value using waveletarima package ===================
            print("[13] Calculating time series features from waveletarima Package...")
            start_time <- tictoc::tic()
            #=========================================== MIRAI function (parallel processing) ===============================================
            # set up mirai for parallel processing
            if (mirai::daemons()$connections == 0){
                # mirai::daemons(parallel::detectCores())
                mirai::daemons(16)
            }

            mirai::everywhere({
                library(data.table)
                })
            
            # Normal Set Up
            windows_ = 252 # one year
            # windows_ = 504 # two year
            price_col = "close"
            # grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
            
            # additional parameters for permutations
            filter = c("haar", "la8")
            MaxARParam = 5
            MaxMAParam = 5
            NForecast = 5

            params <- expand.grid(filter = filter,
                MaxARParam = MaxARParam,
                MaxMAParam = MaxMAParam,
                NForecast = NForecast, stringsAsFactors = FALSE)

            colnames(params) <- c("filter", "MaxARParam", "MaxMAParam", "NForecast")
            # ======================================= the code to be runned =======================================
            results_dt <- self$run_code(symbol_df, windows_, price_col, self$rolling_function, params)
            # results_dt <- data.table::rbindlist(c(try_list,try_list2), use.names = TRUE, fill = TRUE)
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            forecast_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Calculating time series features from waveletarima Package ", forecast_time, " Seconds"))
            rm(start_time, windows_, price_col,filter, MaxARParam, MaxMAParam, NForecast, params, results_dt, new_cols, end_time, forecast_time)
            mirai::daemons(0) # stop the active daemon process cleanly and clean up background workers
            # return(res_list)s
            data.table::setkey(ohlcv, symbol, date)
            return(ohlcv) # might be return(ohlcv)
        }
    )
)

# ================================================================ END OF CODE, TEST NOW ================================================================
# feature_loader <- ohlcv_features_basic$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")
# prepare_df <- feature_loader$other_finfeatures_indicators(df) #test2: 596.379 seconds for 2 ticker

# tail(prepare_df["a"])
# tail(prepare_df["aa"])
# tail(prepare_df["aaa"])

# unique(prepare_df$symbol)[3]

# ================================================================ END OF CODE, TEST NOW ================================================================
# # start_time <- tictoc::tic()
# tsfeature_dt <- data.table::copy(df)
# tsfeature_dt <- tsfeature_dt["aapl"]
# n_rows <- nrow(tsfeature_dt)
# window <- 252 # size of rolling window

# forecast_type <- c("autoarima", "nnetar", "ets") # best arima model, feed-forward neural network model, Exponential smoothing state space model : neural network takes a very long time
# h = 10 # forecast horizon


# y <- forecast::ets(na.omit(tsfeature_dt[, close]))
# y <- as.data.table(forecast::forecast(y, h))


# forecast_type <- c("autoarima", "ets") # suppose to have "nnetar", but it takes too long (bc it is a neural network)
# h = 10 # forecast horizon

# params <- expand.grid(
#     forecast_type = forecast_type,
#     h = h, stringsAsFactors = FALSE)

# colnames(params) <- c("forecast_type", "h")

# params$forecast_type[2]
# params$h[2]

# y - tail(tsfeature_dt[, close], 1)



# filter = c("haar", "la8")
# MaxARParam = 5
# MaxMAParam = 5
# NForecast = 5

# params <- expand.grid(filter = filter,
#     MaxARParam = MaxARParam,
#     MaxMAParam = MaxMAParam,
#     NForecast = NForecast, stringsAsFactors = FALSE)

# params
# for (i in seq_along(params)){
#     print(params$filter[i])
# }

# params
