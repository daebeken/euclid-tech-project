# [2] TIME SERIES MODEL: TIME VARYING GENERAL AUTOREGRESSIVE CONDITIONAL HETEROSKEDASTICITY MODEL (TVGARCH)
# ROLLINGTVGARCH IS OK
# TVGARCH: time varying generalised autoregressive conditional heteroskedasticity
# https://cran.r-project.org/web/packages/tvgarch/index.html
# comment out smaller_symbol_list to run on all the symbols
# TVGARCH TAKES TOO LONG: LINE 110: I RAN AT LEAST 7 HOURS FOR AAPL: made it more efficient with Mirai, takes about 4 minutes to run
# https://github.com/cran/tvgarch/blob/master/R/tvgarch.R 

# Estimates a multiplicative time-varying GARCH model with exogenous variables, capturing both:
# [1] order.g : Non-stationary long-term volatility trends via transition variables (xtv)
# [2] order.h = c(1,1,0) : Short-term conditional heteroscedasticity via a GARCH(p,q,r) structure

# @ outputs: predictions based on the Quasi Maximum Likelihood (ML) estimation
# @ outputs: the coefficients for the models where:
# short term: intercept.h , arch1, garch1
# long term: intercept.g, size1, speed1, location1

# the outputs is dependent on window: if window size too small, it breaks the optimisation function and throws the error: 
# Error in constrOptim(theta = par.ini, f = tvgarchObj, grad = NULL, ui = ui.gh, initial value is not in the interior of the feasible region

# recommended window size: 504 (2 years) (lesser NA values as compared to a window size of 252 (1 year)): furthermore, NO SYMBOL has less than 504 rows: will work every single time


# PROS: TV-GARCH overcomes the stationary volatility process assumption that GARCH has by allowing the volatility parameters—and thus both conditional and unconditional variance—to change smoothly over time.
# CONS: many missing values (NA) due to lack of data: while 504 data point seems enough, it may not be enough to satisfy the optimisation constraints for the model

# @description: uses returns to fit the TVGARCH model
#======= Main Class =======
# create R6 class
feature_three <- R6::R6Class(
	"feature_three",
	private = list(
		load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch")
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
			# @description: the main algorithm for TVGARCH
			# @params: x is the dataframe, containing date, symbol and ohlcv
			# @params: window: size of the rolling window
			# @params: price_cols: the price column to use to compute returns
			# @returns: template_cols values, based on TVGARCH model
			template_cols <- c("pred_1", "pred_n", "pred_mean",
				"intercept.g", "size1", "speed1","location1", "intercept.h", "arch1", "garch1")
			
			empty_row <- data.table::as.data.table(
				setNames(as.list(rep(NA_real_, length(template_cols))), template_cols)
			)
			# compute returns
			if (length(unique(x$symbol)) > 1) return(NA)
			x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1] # compute returns
			
			fit <- tryCatch(tvgarch::tvgarch(na.omit(x$returns) * 100, turbo = FALSE), error = function(e) NULL) # turbo = TRUE
			if (is.null(fit)) return(empty_row)

			pred <- as.numeric(predict(fit))
			coefs <- coef(fit)

			results <- data.table::as.data.table(
				t(setNames(
					c(pred[1], tail(pred, 1), mean(pred, na.rm = TRUE), unname(coefs)),
					template_cols
				))
			)
			# colnames(results) <- paste(colnames(results), window, sep = "_")
			return(results[])
		},
		#=========================================== MAIN CODE TO RUN ===============================================
		run_code = function(subset_df, windows_, price_col, rolling_function){
			# @description: uses the mirai package to integrate asynchronous parallel programming into the script
			# @params: forcing windows to be 504 and price column to be close
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
					price_col = price_col,
					rolling_function = rolling_function
				)
			)[]
			res_matrix <- do.call(rbind, results)
			stat_names <- colnames(res_matrix)

			for (j in seq_along(stat_names)) {
				colname <- paste0("tvgarch_", stat_names[j], "_", windows_)
				result_dt[indices, (colname) := res_matrix[[j]]]
			}

			result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
			new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
			subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
			return(subset_df)
		},
        
        other_finfeatures_indicators = function(symbol_df) {
			# @description: consolidating all functions together to run as 1 function
            ohlcv <- data.table::copy(symbol_df)
			data.table::setkey(ohlcv, symbol, date) # added this in 020825
            data.table::setDTthreads(threads = 0, throttle = 1)
            setorder(ohlcv, symbol, date)
            
            print("=================== COMPUTING TVGARCH INDICATORS ===================")
			#=================== OTHER INDICATORS 5: Time Varying General Autoregressive Score (TVGARCH) Model ===================
			print("[5] Calculating Other Indicators Time Varying General Autoregressive Score (TVGARCH) Model...")
            start_time <- tictoc::tic()
			#=========================================== MIRAI function (parallel processing) ===============================================
            # set up mirai for parallel processing
			if (mirai::daemons()$connections == 0){
				# mirai::daemons(parallel::detectCores())
				mirai::daemons(16) # force the daemons
			}

			mirai::everywhere({
				library(data.table)
				})
			
			# Normal Set Up
			if (nrow(symbol_df) > 504){
				windows_ = 504
			} else {
			   windows_ = 252
			}

			price_col = "close"
			# ======================================= the code to be runned =======================================
			results_dt <- self$run_code(symbol_df, windows_, price_col, self$rolling_function)
			data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

			end_time <- tictoc::toc()
			tvgarch_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Time Varying General Autoregressive Score (TVGARCH) ", tvgarch_time, " Seconds"))
			rm(start_time, windows_, price_col, results_dt, new_cols, end_time, tvgarch_time)
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
# aapl_df <- df[symbol=="aapl"]
# prepare_df <- feature_loader$other_finfeatures_indicators(aapl_df) #test2
# view(prepare_df)

# df["aa"]

# df %>% group_by(symbol) %>% filter(n() < 504) %>% distinct(symbol) %>% count()

# head(prepare_df["a"], 850)
# head(prepare_df["aa"], 6400)

# # ========= WORKS ===========
# aapl_df <- df["aapl"]
# windows_ <- 256 # 1 year


# Precompute returns once
# aapl_df[, returns := (close / data.table::shift(close, 1)) - 1, by = symbol]
# aapl_df = aapl_df[1:260] # test (subset of data)
# n_rows <- nrow(aapl_df)

# TEST
# Initialize result_dt with required structure
# result_dt <- data.table(date = aapl_df$date)
# indices <- (windows_ + 1):n_rows

# for (i in indices){
# 	# first run: first indice : 257
# 	end_indice <- i

# 	window_data <- aapl_df[(end_indice - windows_ + 1):end_indice]
# 	y <- tvgarch::tvgarch(window_data$returns * 100, turbo = TRUE)

# 	pred <- as.numeric(predict(y))
# 	coefs <- coef(y)
# 	results <- as.data.table(pred_1 = pred[1], pred_n = tail(pred, 1), pred_mean = mean(pred, na.rm = TRUE), t(coefs))
	
# 	colnames(results) <- paste0("tvgarch_", colnames(results))
# 	colnames(results) <- paste(colnames(results), windows_, sep = "_")

# 	stat_names <- colnames(results)
# 	for (j in seq_along(stat_names)) {
# 		result_dt[end_indice, (stat_names[j]) := results[[j]]]
# 	}
# }

# result_dt[, symbol := window_data$symbol[1]]
# new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
# aapl_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]

# if (mirai::daemons()$connections == 0){
# 	mirai::daemons(parallel::detectCores())
# }

# mirai::everywhere({
# 	library(data.table)
# 	})

# rolling_function = function(x, window, price_col){
# 	template_cols <- c("pred_1", "pred_n", "pred_mean",
# 		"intercept.g", "size1", "speed1","location1", "intercept.h", "arch1", "garch1")
	
# 	empty_row <- data.table::as.data.table(
#         setNames(as.list(rep(NA_real_, length(template_cols))), template_cols)
#     )
# 	# compute returns
# 	if (length(unique(x$symbol)) > 1) return(NA)
#     x[, returns := get(price_col) / data.table::shift(get(price_col)) - 1] # compute returns
	
# 	fit <- tryCatch(tvgarch::tvgarch(na.omit(x$returns) * 100, turbo = FALSE), error = function(e) NULL) # turbo = TRUE
# 	if (is.null(fit)) return(empty_row)

# 	pred <- as.numeric(predict(fit))
# 	coefs <- coef(fit)

# 	results <- data.table::as.data.table(
#         t(setNames(
#             c(pred[1], tail(pred, 1), mean(pred, na.rm = TRUE), unname(coefs)),
#             template_cols
#         ))
#     )
# 	# colnames(results) <- paste(colnames(results), window, sep = "_")
# 	return(results[])
# }

# mirai_function = function(i, dt, windows_, price_col, rolling_function){
# 	data.table::setDTthreads(1)
# 	window_data <- dt[(i - windows_ + 1):i]
# 	rolling_function(window_data, windows_, price_col)
# 	# out <- tryCatch(
# 	# 	rolling_function(window_data, windows_, price_col),
# 	# 	error = function(e){
# 	# 		data.table::as.data.table(
# 	# 			setNames(as.list(rep(NA_real_, length(template_cols)), template_cols))
# 	# 		)
# 	# 	})
	
# 	# miss <- setdiff(template_cols, names(out))
#     # if (length(miss)) {out[, (miss) := NA_real_]}
# 	# out
# 	}


# run_code = function(subset_df, windows_, price_col, rolling_function){
# 	n_rows <- nrow(subset_df)
# 	indices <- (windows_ + 1):n_rows

# 	result_dt <- data.table(date = subset_df$date)

# 	results <- mirai::mirai_map(
# 		.x = indices,
# 		.f = mirai_function,
# 		.args = list(
# 			dt = subset_df,
# 			windows_ = windows_,
# 			price_col = price_col,
# 			rolling_function = rolling_function
# 		)
# 	)[]
# 	res_matrix <- do.call(rbind, results)
#     stat_names <- colnames(res_matrix)

# 	for (j in seq_along(stat_names)) {
# 		colname <- paste0("tvgarch_", stat_names[j], "_", windows_)
# 		result_dt[indices, (colname) := res_matrix[[j]]]
# 	}

# 	result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
# 	new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
# 	subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
# 	return(subset_df)
# }

# aapl_df <- df["aapl"] # to be removed
# # test_df <- aapl_df[1:400]
# windows_ <- 504 # 2 year # to be removed
# price_col <- "close"

# # test_list <- run_code(test_df, windows_, price_col, rolling_function)
# # new_cols <- colnames(test_list)[!colnames(test_list) %in% c(colnames(aapl_df), "returns")]




# # aapl_df <- aapl_df[1:260]

# results_list <- run_code(aapl_df, windows_, price_col, rolling_function)
# nrow(results_list)

# # ====== NEW TEST =========
# # aapl_df <- df["aapl"] # to be removed
# # test_df <- aapl_df[1:505]
# # windows_ <- 504 # 2 year # to be removed
# # price_col <- "close"


# # # rolling_function(test_df, windows_, price_col)

# # n_rows <- nrow(test_df)
# # indices <- (windows_ + 1):n_rows # 44 numbers

# # first_indice <- indices[1]

# # window_data <- test_df[(first_indice - windows_ + 1):first_indice]
# # # rolling_function(window_data, windows_, price_col)

# # window_data[, returns := get(price_col) / data.table::shift(get(price_col)) - 1] # compute returns

# # y <- tryCatch(tvgarch::tvgarch(na.omit(window_data$returns) * 100, turbo = TRUE), error = function(e) NA)



# # tvgarch::tvgarch(na.omit(window_data$returns)*100, turbo = FALSE)
