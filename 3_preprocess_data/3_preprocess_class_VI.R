# [5] TIME SERIES MODEL: theft package contains methods that produces time series features
# 8: working on theft package
# https://cran.r-project.org/web/packages/theft/index.html

# will be using catch22 and tsfresh
# will be using catch22 (24 features bc with mean and std) and feasts (42 features)

# [1] based on the research paper, catch22, a feature set designed to reduce within-set redundancy, displayed the least within-set redundancy (most efficient at capturing 
# time series characteristics in comparison to the most comprehensive feature set, hctsa)

# [2] based on the research paper, tsfresh was marked as the most unique feature set, largely driven by a large number of features that capture the raw outputs of a fast 
# Fourier transform as real and imaginary components, magnitudes, and phase angles ("new features", not seen in catch22)

# @description: on the CLOSING PRICE

#======= Main Class =======
# create R6 class
feature_six <- R6::R6Class(
	"feature_six",
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch", "quarks", "ufRisk", "theftdlc", "theft")
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
        rolling_function = function(x, window, price_col){
        # @description: the main algorithm for Quarks
        # @params: x is the dataframe, containing date, symbol and ohlcv
        # @params: window: size of the rolling window
        # @params: methods: the time series feature to be computed out
        # @returns: values given by that time series method
            # main algorithm, dependent on method
            
            feature_sets <- c("catch22", "feasts") # # c("catch22", "feasts", "tsfeatures", "Kats", "tsfresh", "TSFEL")
            
            y <- suppressWarnings(
                data.table::as.data.table(
                    theft::calculate_features(
                        data = x,
                        id_var = "symbol",
                        time_var = "date",
                        values_var = price_col,
                        feature_set = feature_sets,
                        catch24 = TRUE
                )
            ))

            y[, var_names := paste(feature_set, names, window, sep = "_")]
            y = data.table::transpose(y[, .(var_names, values)], make.names = TRUE)
            results = data.table::as.data.table(y)
            colnames(results) <- gsub(" |-", "_", colnames(results))
            return(results)
        },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, rolling_function){
            # @description: uses the mirai package to integrate asynchronous parallel programming into the script

            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional
            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col) {
                data.table::setDTthreads(2)  # Allow light multithreading inside workers
                out <- lapply(index_chunk, function(i) {
                    window_data <- dt[(i - windows_ + 1):i]
                    rolling_function(window_data, windows_, price_col)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(
                    dt = subset_df,
                    windows_ = windows_,
                    rolling_function = rolling_function,
                    price_col = price_col
                )
            )[]

            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("theft_", stat_names[j])
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
            
            print("=================== [8] COMPUTING time series features from tsfeatures Package ===================")
            #=================== OTHER INDICATORS 8: VaR and Expected Shortfall from Quarks Package ===================
            print("[8] Calculating time series features from theft Package...")
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
            price_col = "close"
            # grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
            # ======================================= the code to be runned =======================================
            # focused on fhs because it is what the author encourages
            # try_list <- lapply(smaller_symbol_list, function(dt){
            #             self$run_code(dt, windows_, price_col, self$rolling_function)})
            
            try_list <- as.data.table(self$run_code(symbol_df, windows_, price_col, self$rolling_function))

            # results_dt <- data.table::rbindlist(c(try_list,try_list2), use.names = TRUE, fill = TRUE)
            # results_dt <- data.table::rbindlist(try_list, use.names = TRUE, fill = TRUE)
            results_dt <- try_list
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            theft_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Calculating time series features from theft Package ", theft_time, " Seconds"))
            rm(start_time, windows_, price_col, try_list, results_dt, new_cols, end_time, theft_time)
            mirai::daemons(0) # stop the active daemon process cleanly and clean up background workers
            # return(res_list)s
            data.table::setkey(ohlcv, symbol, date)
            return(ohlcv) # might be return(ohlcv)
        }
    )
)

# # ================================================================ END OF CODE, TEST NOW ================================================================
# feature_loader <- ohlcv_features_basic$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")
# aapl_df <- df[symbol=="aapl"]
# prepare_df <- feature_loader$other_finfeatures_indicators(aapl_df) #test2
# view(prepare_df)
# class(prepare_df)



# feature_loader <- ohlcv_features_basic$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")
# prepare_df <- feature_loader$other_finfeatures_indicators(df) #test2

# colnames(prepare_df["a"])

# # ================================================================ END OF CODE, TEST NOW ================================================================
# # start_time <- tictoc::tic()
# theft_dt <- data.table::copy(df)
# theft_dt <- theft_dt["aapl"]
# n_rows <- nrow(theft_dt)
# window <- 252 # size of rolling window

# feature_loader$rolling_function(theft_dt, window, "close")

# # theft::init_theft(
# #     python_path = '/Library/Frameworks/Python.framework/Versions/3.13/bin/python3',
# #     venv_path = '/Library/Frameworks/Python.framework/Versions/3.13'
# # )
# feature_sets <- c("catch22", "feasts")

# # working on theft package
# a <- theft::calculate_features(
# 	data = theft_dt,
# 	id_var = "symbol",
# 	time_var = "date",
# 	values_var = "close",
# 	feature_set = feature_sets,
#     catch24 = TRUE) # c("catch22", "feasts", "tsfeatures", "Kats", "tsfresh", "TSFEL")

# b <- theft::calculate_features(
# 	data = theft_dt,
# 	id_var = "symbol",
# 	time_var = "date",
# 	values_var = "close",
# 	feature_set = "feasts",
#     catch24 = TRUE) # c("catch22", "feasts", "tsfeatures", "Kats", "tsfresh", "TSFEL")
