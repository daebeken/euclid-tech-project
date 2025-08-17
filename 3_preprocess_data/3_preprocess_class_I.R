# FOR TESTING: THESE WORKS, TO BE USED IN THE PARENT FOLDER
#' @description creates features from 3 packages: binomialtrend, VSE, exuber
#' @description creates features ONE SYMBOL AT A TIME
#' @note [utilises closing price] binomialtrend
#' @note [utilises returns] vse4ts: variance scale exponent (VSE)
#' @note [utilises closing price] exuber
#' @warning REMEMBER TO DELETE THE LOAD DATA FUNCTION AWAY AFTER THIS

#======= Main Class =======
# create R6 class
feature_one <- R6::R6Class(
	"feature_one",
	private = list(
		load_dependencies = function(){
			#' @description loads the R packages used in this class
            #' @note if the packages is missing in the laptop, install the packages for them
			required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "backCUSUM")
			
			for (package in required_packages){
                if (!require(package, character.only = TRUE, quietly = TRUE)){
                    message(paste0("Installing Missing Package: ", package))
                    install.packages(package, dependencies = TRUE)
                }
                suppressPackageStartupMessages(
                    library(package, character.only = TRUE, warn.conflicts = FALSE)
                )
            }
		}
	),
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
		#=== FIRST FUNCTION ===
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
        
        other_finfeatures_indicators = function(symbol_df) {
			#' @description extracts features on a per symbol basis
			#' @note no parallel computing involved here because I will want to use that in the consolidation phase i.e. parallel computing in the symbol
            
			ohlcv <- data.table::copy(symbol_df)
			windows_ = self$windows # initialize the names of the different windows
            data.table::setDTthreads(threads = 0, throttle = 1)
            at_ <- if (is.null(self$at)) 1:nrow(ohlcv) else self$at
            setorder(ohlcv, symbol, date)
			#=================== OTHER INDICATORS 1: BINOMIAL TREND =================== 915.064
			# T Test: if positive : uptrend , if negative, downtrend, strength of trend dependent on magnitude
			binomial_trend_dt <- data.table::copy(ohlcv)
			windows_ = self$windows
			outputs = c("trend", "pvalue") # can be used to adjust the parameter in PerformanceAnalytics::skewness()
            grid <- base::expand.grid(output = outputs, window = windows_, stringsAsFactors = FALSE)
            new_cols <- paste0("binomial_trend_", grid$output, "_", grid$window)

			close_vec <- binomial_trend_dt$close

			for (w in self$windows) {
				trend_col <- paste0("binomial_trend_trend_", w)
				pval_col  <- paste0("binomial_trend_pvalue_", w)

				# Apply frollapply per rolling window
				binomial_trend_dt[, (trend_col) := frollapply(close_vec, n = w, align = "right",
					FUN = function(x) binomialtrend::binomialtrend(x)$parameter)]

				binomial_trend_dt[, (pval_col) := frollapply(close_vec, n = w, align = "right",
					FUN = function(x) binomialtrend::binomialtrend(x)$p.value)]}
			
			ohlcv[binomial_trend_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            rm(new_cols, outputs, grid)
			#=================== OTHER INDICATORS 2: Variance Scale Exponent (VSE) =================== 3371.852 Seconds
			# weak/strong refers to the variance scale exponent which determines whether it is short, long term memory or noise, 
			# wnoise determines whether it is white noise, sl determines whether it is long memory
			# A negative value usually signals a failure in model estimation
			# Answers the following question: does the return exhibit short, long term memory or it is just noise?
			vse_dt <- data.table::copy(ohlcv)
			vse_dt[, ("returns") := close / data.table::shift(close, n = 1) -1]
			vse_dt <- na.omit(vse_dt, cols = "returns")  # to ensure output does not give NA
			
			windows_ <- self$windows # arbitrary number first
			outputs <- c("weak", "strong", "wnoise", "wnoise_pvalue", "sl", "sl_pvalue")
			grid <- expand.grid(output = outputs, window = windows_, stringsAsFactors = FALSE)
			new_cols <- paste0("vse_", grid$output, "_", grid$window)

			return_vec <- vse_dt$returns
			dates <- vse_dt$date

			for (w in windows_) {
				vse_dt[, paste0("vse_weak_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::vse(na.omit(x), type = "weak") else NA_real_)]

				vse_dt[, paste0("vse_strong_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::vse(na.omit(x), type = "strong") else NA_real_)]

				vse_dt[, paste0("vse_wnoise_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::Wnoise.test(na.omit(x))$Wnoise else NA_real_)]

				vse_dt[, paste0("vse_wnoise_pvalue_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::Wnoise.test(na.omit(x))$p.value else NA_real_)]

				vse_dt[, paste0("vse_sl_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::SLmemory.test(na.omit(x))$SLmemory else NA_real_)]

				vse_dt[, paste0("vse_sl_pvalue_", w) := frollapply(return_vec, w, align = "right",
				FUN = function(x) if (any(!is.na(x))) vse4ts::SLmemory.test(na.omit(x))$p.value else NA_real_)]
			}

			ohlcv[vse_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]

            rm(new_cols, vse_dt)
			#=================== OTHER INDICATORS 3: Exuber =================== 708 Seconds
			# ADF: Tests for unit roots (non-stationarity consistent with a random walk): whether the price is stationary or not
			# SADF: Detects the presence of a single period of explosive behavior (e.g., asset bubbles)
			# GSADF: Generalizes SADF to detect multiple periods of explosiveness (i.e., multiple bubbles)
			# BADF: Backward ADF test – tests for explosiveness starting from a fixed endpoint and moving the start point backward
			# BSADF: Backward Sup ADF – the supremum of BADF statistics over varying start points for a fixed endpoint; used in GSADF
			# Interpretation: Higher test statistics suggest stronger evidence of explosive behavior, 
			# but significance is determined by comparing against simulated critical values (e.g., 95% level)
			exuber_dt <- data.table::copy(ohlcv)
            windows_ <- 22  # Keep test window: window = 5 too little: will give a lot of NA
            stat_types <- c("adf", "sadf", "gsadf", "badf", "bsadf")
            new_cols <- unlist(lapply(windows_, function(w) paste0("exuber_", stat_types, "_", w)))

            exuber_function = function(close_vec, window) {
                minw <- max(floor(0.1 * window) + 1, 20)  # Minimum 20 periods required
                if (length(close_vec) < minw) {
                    return(rep(NA_real_, length(stat_types)))
                }
                
                tryCatch({
                    radf_obj <- exuber::radf(close_vec, minw = minw)
                    # Return vector in correct order: adf, sadf, gsadf, badf, bsadf
                    c(tail(radf_obj$adf, 1),
                    tail(radf_obj$sadf, 1),
                    tail(radf_obj$gsadf, 1),
                    tail(radf_obj$badf, 1),
                    tail(radf_obj$bsadf, 1))
                }, error = function(e) {
                    return(rep(NA_real_, length(stat_types)))
                })
            }

            n_rows <- nrow(exuber_dt)
			
			for (col in new_cols) exuber_dt[, (col) := NA_real_]
            if (n_rows < max(windows_)) return(result_dt)
                
			# Process each window
			for (w in windows_) {
				start_index <- max(w, 20)  # Ensure minw requirement
				
				# Vectorized approach for efficiency
				results <- lapply(start_index:n_rows, function(i) {
					window_close <- exuber_dt$close[(i-w+1):i]
					exuber_function(window_close, w)
				})
				
				# Convert to matrix and assign to result_dt
				res_matrix <- do.call(rbind, results)
				for (j in seq_along(stat_types)) {
					colname <- paste0("exuber_", stat_types[j], "_", w)
					exuber_dt[start_index:n_rows, (colname) := res_matrix[, j]]
				}
			}
            
            # Merge with original ohlcv data
            setkey(ohlcv, symbol, date)
            setkey(exuber_dt, symbol, date)
            ohlcv[exuber_dt, (new_cols) := mget(paste0("i.", new_cols))]
            
			rm(windows_, stat_types, new_cols, exuber_function)
			
			#=================== OTHER INDICATORS 4: backCUSUM ===================
			#' @description window size 252 (1 year)
			#' @note takes TOO LONG TO RUN
			#' @description The backward CUSUM detector considers the recursive residuals in reverse chronological order, whereas the stacked backward CUSUM detector 
			#' sequentially cumulates a triangular array of backwardly cumulated residuals. 
			#' @link https://www.cambridge.org/core/journals/econometric-theory/article/abs/backward-cusum-for-testing-and-monitoring-structural-change-with-an-application-to-covid19-pandemic-data/A91390050C0ECB59C1F91F2E1C10B4DE
			
			# backcusum_dt <- data.table::copy(ohlcv)
			# backcusum_dt[,returns := close / data.table::shift(close) - 1]
			# window_size <- 252 # one-year look-back
			# start_index <- window_size

            # # helper now takes a numeric vector
			# rolling_function <- function(ret_vec, alternative = "greater", return_power = 1) {
			# 	y <- na.omit(ret_vec ^ return_power)
			# 	if (length(y) < 10) return(NULL)  # too little data → skip
			# 	sbq <- backCUSUM::SBQ.test(y ~ 1, alternative = alternative)
				
			# 	data.table(
			# 		backcusum_statistic = sbq$statistic
			# 		# backcusum_rejection_10bp = as.integer(sbq$rejection["10%"]), # returns NA
			# 		# backcusum_rejection_5bp = as.integer(sbq$rejection["5%"]), # returns NA
			# 		# backcusum_rejection_1bp = as.integer(sbq$rejection["1%"]) # returns NA
			# 	)
			# }

            # # parallel rolling
			# results <- parallel::mclapply(seq.int(start_index, nrow(backcusum_dt)), mc.cores = self$num_cores,
			# 	FUN = function(i) {
			# 		out <- rolling_function(
			# 			ret_vec = backcusum_dt$returns[(i - window_size + 1):i])
			# 		if (is.null(out)) return(NULL)
			# 		out[, `:=`(symbol = backcusum_dt$symbol[i], date = backcusum_dt$date[i])]
			# 		out
			# 	})

			# results_dt <- rbindlist(results, use.names = TRUE, fill = TRUE)

			# if (nrow(results_dt)) {
			# 	setkey(results_dt, symbol, date)
			# 	cols_to_add <- setdiff(names(results_dt), c("symbol", "date"))
			# 	ohlcv[results_dt, (cols_to_add) := mget(paste0("i.", cols_to_add))]}

			# #=================== Return Output ===================
			# # num_cols <- setdiff(names(ohlcv), c("symbol", "date"))
			data.table::setkey(ohlcv, symbol, date)
			return(ohlcv)
            }
    )
)

#'/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/stocks_daily.duckdb'
#======= TEST =======
# feature_loader <- feature_one$new() # new instance
# # df <- feature_one$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")

# database_directory <- paste0("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "/" ,"stocks_daily", ".duckdb")
# con <- dbConnect(duckdb::duckdb(), dbdir = database_directory, read_only = FALSE)
# df <- as.data.table(dbReadTable(con, "stocks_daily"))
# symbol_one <- unique(df$symbol)[1]

# symbol_df <- df[symbol == symbol_one]

# df1 <- feature_loader$other_finfeatures_indicators(symbol_df)

# symbol_df[,returns := close / data.table::shift(close) - 1]

# rolling_function = function(x, window, price_col, alternative, return_power) {
#       # check if there is enough data
# 	if (length(unique(x$symbol)) > 1) {
# 		return(NA)
# 	}

# 	# calculate arima forecasts
# 	y <- na.omit(x$returns^return_power)
# 	y <- backCUSUM::SBQ.test(as.formula('y ~ 1'), alternative = alternative)
# 	results <- c(y[['statistic']], as.integer(y[['rejection']]))
# 	names(results) <- c("statistics", paste0("backcusum_rejections_", as.numeric(names(y[['rejection']])) * 1000))
# 	results <- as.data.table(as.list(results))
# 	colnames(results) <- paste("backcusum", window, "alternative_greater_return_power_1", colnames(results), sep = "_")
# 	return(results)
#     }

# view(rolling_function(symbol_df, 252, "close", "greater", 1)) #OK





# prepare_df <- feature_loader$other_finfeatures_indicators(df) #test2
# tail(prepare_df)

# prepare_df %>% filter(binomial_trend_pvalue_22 == NA)

# prepare_df["aapl"]
# # w = 5


# feature_loader <- ohlcv_features_basic$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily")
# aapl_df <- df[symbol=="aapl"]
# prepare_df <- feature_loader$other_finfeatures_indicators(aapl_df) #test2
# view(prepare_df)
# view(aapl_df)

# result_dt <- data.table(date = aapl_df$date)
# trend_col <- paste0("binomial_trend_trend_", w)
# pval_col  <- paste0("binomial_trend_pvalue_", w)

# # Apply frollapply per rolling window
# result_dt[, (trend_col) := frollapply(close_vec, n = w, align = "right",
# 	FUN = function(x) binomialtrend::binomialtrend(x)$parameter)]

# result_dt[, (pval_col) := frollapply(close_vec, n = w, align = "right",
# 	FUN = function(x) binomialtrend::binomialtrend(x)$p.value)]

# result_dt[, symbol := "aapl"]

# apply_binomialtrend_trend = function(df){
# 	output <- binomialtrend::binomialtrend(df)
# 	results <- output$parameter
# 	names(results) <- "trend"
# 	return(results)
# }

# apply_binomialtrend_pvalie = function(df){
# 	output <- binomialtrend::binomialtrend(df)
# 	results <- output$$p.value
# 	names(results) <- "trend"
# 	return(results)
# }

# data.table::frollapply(x = aapl_df$close, n = 50, align = "right", FUN = apply_binomialtrend_trend)

# df2 <- data.table::copy(df)
# aapl_df <- df2["aapl"]
# aapl_df[, ("returns") := close / data.table::shift(close, n = 1) -1, by = symbol]
# return_vec <- aapl_df$returns
# return_vec <- na.omit(return_vec) # to ensure output does not give NA

