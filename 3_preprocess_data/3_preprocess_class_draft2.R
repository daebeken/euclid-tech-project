# USING THIS FOR PIPELINE 3
# HARD CODING ON SKEWNESS AND KURTOSIS
# Due to some columns being logical type (not sure why), I had to force all columns to be as.numeric() except symbol and date
#' @description: prepares basic ohlcv pricing features (TWAP, Days since N-day High and Low, rolling minimum return etc) and technical indicators
#' @note: use prepare_df and prepare_technical_indicators
#' @example # prepare_df <- feature_loader$prepare_data(aapl_df) # basic indicators
#' @example # prepare_technical_indicators <- feature_loader$prepare_technical_indicators(aapl_df) # technical indicators
#' @references https://github.com/MislavSag/finfeatures/blob/main/R/OhlcvFeaturesDaily.R

#======= Main Class =======
# create R6 class
technical_indicators <- R6::R6Class(
	"technical_indicators",
	private = list(
		load_dependencies = function(){
			#' @description loads the R packages used in this class
            #' @note if the packages is missing in the laptop, install the packages for them
			required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "parallel", "tictoc", "R.utils", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
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
		#=========================================== FIRST FUNCTION ===========================================
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
		#=========================================== SECOND FUNCTION ===========================================
		load_example_company = function(df, stock_name, close_column = "adj_close"){
			# @description: [used if needed] for debugging purposes, used to test on one company/ticker
			# @params df: the database containing OHLCV, symbol (stock name) and date
			# @params stock_name: must be in the ticker form, regardless of upper or lower casing
			# @params close_column: choose the close column: either adj_close or close
			# @returns ohlcv for the specific company/ticker
			# checks
			checkmate::assert_data_table(df)
			checkmate::assert_character(stock_name)
			checkmate::assert_character(close_column)
			
			df1 <- data.table::copy(df) # creates a deep copy to prevent changes to new data.table() from affecting the original
			ohlcv <- df1 %>% filter(symbol == tolower(stock_name)) # extract the stock's data
			
			close_options <- c("close", "adj_close") # filter out the chosen close price
			remove_close_column <- close_options[!(close_options %in% close_column)]
			ohlcv[, (remove_close_column) := NULL]
			tryCatch(
				setnames(ohlcv, old = "adj_close", new = "close"), # in the event where the input close price is the close price
				error = function(e) {}
				)
			setcolorder(ohlcv, c("date", "symbol", "open", "high", "low", "close", "volume"))
			# remove rows with super small values
			ohlcv <- ohlcv[open > 0.00001 & high > 0.00001 & low > 0.00001 & close > 0.00001]
			return(ohlcv)},
		#=========================================== THIRD FUNCTION ===========================================
		prepare_data = function(symbol_df){
			# @ IMPORTANT assumption: we are using close instead of adj_close for any manipulation of data
			# checks
			checkmate::assert_data_table(symbol_df)
			checkmate::assert_true(symbol_df[, .N, by = symbol][, all(N > max(self$windows))]) # ensures that all symbols have rows that are > window size
			checkmate::test_subset(c("symbol","open","high","low","close"), colnames(symbol_df)) # check if all the necessary columns are there
			
			ohlcv <- data.table::copy(symbol_df) # creates a deep copy to prevent changes to new data.table() from affecting the original
			#setkey(ohlcv, c("symbol")) # set the key to be "symbol" which is the ticker and date
			windows_ = self$windows # initialize the names of the different windows
			
            # plan(multisession) 
			data.table::setDTthreads(threads = 0, throttle = 1) # for parallel processing: 0 means use all logical CPUs available
			if(is.null(self$at)){
				at_ = 1:nrow(ohlcv)} # row index which is used to calculate indicators
				else{
				at_ = self$at}

			setorder(ohlcv, symbol, date)
			ids = c("date", "symbol")
			start_cols = ohlcv[, colnames(.SD)] # the original ohlcv columns

			print("Preparing Basic Predictors")
			#=================== (Normal) Returns ===================
			print("Calculating Normal Returns...")
			start_time <- tictoc::tic()
			w_ = c(1:5, 5*2, 22*(1:12), 252 * 2, 252 * 4)
			w_ = sort(unique(c(windows_, w_))) # computing returns for different time periods
			new_normal_return_cols = paste0("returns_", w_)
			ohlcv[, (new_normal_return_cols) := lapply(w_, function(w) as.numeric(close / data.table::shift(close, n = w) - 1)), by = symbol]

			end_time <- tictoc::toc()
			normal_return_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Normal Returns: ", normal_return_time, " Seconds"))
			rm(start_time, w_, new_normal_return_cols, end_time, normal_return_time) # need to remove to conserve memory space
			#=================== (Logarithmic) Returns ===================
			print("Calculating Logarithmic Returns...")
			start_time <- tictoc::tic()
			w_ = c(1:5, 5*2, 22*(1:12), 252 * 2, 252 * 4)
			w_ = sort(unique(c(windows_, w_))) # computing returns for different time periods
			new_log_return_cols = paste0("log_returns_", w_) # NEED CHANGE THIS
            ohlcv[, (new_log_return_cols) := lapply(w_, function(w) as.numeric(log(close / data.table::shift(close, n = w)))), by = symbol]
			
            end_time <- tictoc::toc()
			log_return_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Log Normal Returns: ", log_return_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, w_, new_log_return_cols, end_time, log_return_time) # need to remove to conserve memory space
			#=================== Time Weighted Average Price (TWAP) ===================
			print("Calculating TWAP...")
			start_time <- tictoc::tic()
			ohlcv[, OC_dist := as.numeric(abs(close - open))]
			ohlcv[, OH_dist := as.numeric(high - open)]
			ohlcv[, OL_dist := as.numeric(open - low)]
			ohlcv[, HL_dist := as.numeric(high - low)]
			ohlcv[, LC_dist := as.numeric(close - low)]
			ohlcv[, HC_dist := as.numeric(high - close)]
			ohlcv[, OHLC_dist := as.numeric(OH_dist + HL_dist + LC_dist)]
			ohlcv[, OLHC_dist := as.numeric(OL_dist + HL_dist + HC_dist)]
			ohlcv[, OH_mean := as.numeric((open + high) * 0.5)]
			ohlcv[, OL_mean := as.numeric((open + low) * 0.5)]
			ohlcv[, HL_mean := as.numeric((high + low) * 0.5)]
			ohlcv[, LC_mean := as.numeric((low + close) * 0.5)]
			ohlcv[, HC_mean := as.numeric((high + close) * 0.5)]
			ohlcv[, OHLC_twap := as.numeric(((OH_dist / OHLC_dist) * OH_mean +
									(HL_dist / OHLC_dist) * HL_mean +
									(LC_dist / OHLC_dist) * LC_mean)), by = symbol]
			ohlcv[, OLHC_twap := as.numeric((((OL_dist / OLHC_dist) * OL_mean) +
									((HL_dist / OLHC_dist) * HL_mean) +
									((HC_dist / OLHC_dist) * HC_mean))), by = symbol]
			ohlcv[, twap := as.numeric((OHLC_twap + OLHC_twap) * 0.5), by = symbol]
			# if (!is.null(at_)) {
			# 	dt_twap = ohlcv[at_, .SD, .SDcols = c(ids, "twap")]
			# 	cols_ = c("OC_dist", "OH_dist", "OL_dist", "HL_dist", "LC_dist",
			# 			"HC_dist", "OHLC_dist", "OLHC_dist", "OH_mean", "OL_mean",
			# 			"HL_mean", "LC_mean", "HC_mean", "OHLC_twap", "OLHC_twap",
			# 			"twap")
			# 	ohlcv[, (cols_) := NULL]
			# } else {
			# 	cols_ = c("OC_dist", "OH_dist", "OL_dist", "HL_dist", "LC_dist",
			# 			"HC_dist", "OHLC_dist", "OLHC_dist", "OH_mean", "OL_mean",
			# 			"HL_mean", "LC_mean", "HC_mean", "OHLC_twap", "OLHC_twap")
			# 	ohlcv[, (cols_) := NULL]
			# }
			end_time <- tictoc::toc()
			twap_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Time Weighted Average Price: ", twap_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, end_time, twap_time) # need to remove to conserve memory space
			#=================== Close ATH Ratio===================
			print("Calculating Close All Time High Ratio...")
			start_time <- tictoc::tic()
            ohlcv[, close_ath := as.numeric(close / base::cummax(fifelse(is.na(high), -Inf, high))), by = symbol]

			end_time <- tictoc::toc()
			close_ath_ratio <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Close All Time High Ratio ", close_ath_ratio, " Seconds")) # NEED CHANGE THIS
			rm(start_time, end_time, close_ath_ratio) # need to remove to conserve memory space
			#=================== Days Since N-Day High and Low: ON "CLOSE" COLUMN ===================
			# Used a subset of the data to do this
			# BOTTLE NECK WHEN USING DATA TABLE DT THREADS: Used parallelisation instead
			print("Calculating Days Since N-Day High and Low: ON 'CLOSE' COLUMN...")
			start_time <- tictoc::tic()
			w_ = c(22, 66, 132, 264) 
			w_ = sort(unique(c(windows_, w_))) # lesser windows to save memory: will have 5 as well bc it is under windows_
            days_since_high_cols = paste0("days_since_high_", w_)
			days_since_low_cols = paste0("days_since_low_", w_)
			cols_ = c(days_since_high_cols, days_since_low_cols)

			nday_ohlcv <- data.table::copy(symbol_df)
			nday_ohlcv <- nday_ohlcv[, .SD, .SDcols = c("symbol", "date", "close")] # use a subset of the data to save memory

            symbol_list <- split(nday_ohlcv, by = "symbol")
			processed_list <- mclapply(symbol_list, function(dt) {
				for(i in days_since_high_cols){
					days = as.numeric(gsub(".*_", "", i))
					dt[, (i) := as.numeric(data.table::frollapply(close, days, FUN = function(x) length(x) - which.max(x)))]}

				for(i in days_since_low_cols){
					days = as.numeric(gsub(".*_", "", i))
					dt[, (i) := as.numeric(data.table::frollapply(close, days, FUN = function(x) length(x) - which.min(x)))]}

				return(dt)
				}, mc.cores = self$num_cores)
			dt_returns1 <- rbindlist(processed_list)
			dt_returns1 <- dt_returns1[, close := NULL]

            ohlcv[dt_returns1, on = .(symbol, date), (cols_) := mget(paste0("i.", cols_))]

			end_time <- tictoc::toc()
			days_since_high_low_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Days since High Low: ", days_since_high_low_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, w_, days_since_high_cols, days_since_low_cols, cols_, nday_ohlcv, symbol_list,processed_list,dt_returns1, end_time, days_since_high_low_time) # need to remove to conserve memory space
			#=================== Rolling Minimum Return COLUMN ===================
			print("Calculating Rolling Minimum Return...")
			start_time <- tictoc::tic()
			w_ = c(5, 5*2, 22*(1:12), 252 * 2, 252 * 4)
			w_ = sort(unique(c(windows_, w_))) # computing returns for different time periods
			new_cols = paste0("min_returns_", w_) # NEED CHANGE THIS
            ohlcv[, (new_cols) := lapply(w_, function(n) {
                as.numeric(data.table::frollapply(returns_1, n, min, na.rm = TRUE, align = "right")) # compare the 1-day returns within the window
            }), by = symbol]
			
			end_time <- tictoc::toc()
			rolling_min_return_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Rolling Minimum Returns: ", rolling_min_return_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, w_, new_cols, end_time, rolling_min_return_time) # need to remove to conserve memory space
			#=================== Volume Percent Change COLUMN ===================
			print("Volume percent changes")
			start_time <- tictoc::tic()
			w_ = c(1:5, 5*2, 22*(1:12), 252 * 2, 252 * 4)
			w_ = sort(unique(c(windows_, w_)))
			new_cols = paste0("volume_", w_)
			ohlcv[, (new_cols) := lapply(w_, function(w) as.numeric(volume / data.table::shift(volume, n = w) - 1)), by = symbol]
			end_time <- tictoc::toc()
			volume_percent_change_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Volume Percent Change: ", volume_percent_change_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, w_, new_cols, end_time, volume_percent_change_time) # need to remove to conserve memory space
			#=================== Volume Concentration COLUMN ===================
			# This ratio highlights how much recent trading activity dominates compared to longer-term trends, offering a proxy for volume-based momentum or accumulation/distribution dynamics.
			print("Calculating Volume Concentration: ON 'Volume' COLUMN...")
			start_time <- tictoc::tic()
			w_ = c(2, 5, 22*(1:12), 252 * 2, 252 * 4)  # Test on this first: try one set first
			#w_ = sort(unique(c(windows_, w_))) # lesser windows to save memory: will have 5 as well bc it is under windows_
			previous_volume_cols <- paste0("previous_", w_, "_volume")
			volume_conc_cols <- paste0("volume_conc_", w_[-length(w_)], "_", w_[-1]) # the number and the next bigger number
			cols_ <- c(previous_volume_cols, volume_conc_cols)
			volume_conc_ohlcv <- data.table::copy(symbol_df)
			volume_conc_ohlcv <- volume_conc_ohlcv[, .SD, .SDcols = c("symbol", "date", "volume")] # use a subset of the data to save memory
            symbol_list <- split(volume_conc_ohlcv, by = "symbol")

			ohlcv[, (previous_volume_cols) := lapply(w_, function(days) {
				as.numeric(frollsum(volume, n = days, align = "right", na.rm = TRUE, fill = NA))}), by = symbol]

			conc_cols <- paste0("volume_conc_", w_[-length(w_)], "_", w_[-1])
			conc_days <- Map(function(a, b) c(a, b), w_[-length(w_)], w_[-1])

			for (i in seq_along(conc_cols)) {
				days_pair <- conc_days[[i]]
				smaller <- min(days_pair)
				larger <- max(days_pair)
				ohlcv[, (conc_cols[i]) := as.numeric(get(paste0("previous_", smaller, "_volume")) / get(paste0("previous_", larger, "_volume"))), by = symbol]
			}

			end_time <- tictoc::toc()
			volume_conc_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Volume Concentration Ratio: ", volume_conc_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, w_, cols_, previous_volume_cols, volume_conc_cols, volume_conc_ohlcv, end_time, volume_conc_time) # need to remove to conserve memory space
			#=================== Bid Ask Spread ===================
			print("Calculating Bid Ask Spread...")
			start_time <- tictoc::tic()
			bid_ask_dt <- data.table::copy(symbol_df)
			bid_ask_dt <- bid_ask_dt[, .SD, .SDcols = c("symbol", "date","open", "high", "low", "close")]
			symbol_list <- split(bid_ask_dt, by = "symbol", keep.by = TRUE)
			methods_ = c("EDGE", "OHL", "OHLC", "AR", "AR2", "CS", "CS2", "ROLL") # test try first 3 first
			spread_list <- mclapply(symbol_list, function(dt) {
				spread_result <- bidask::spread(
					as.xts.data.table(dt[, .SD, .SDcols = c("date", "open", "high", "low", "close")]), # need to be xts object to get time series column else object returned have no date column
					width = 132, # rolling window: NEED TO CHANGE: 132 bc half a year
					method = methods_
				)
				result <- as.data.table(spread_result)
				result[, symbol := dt$symbol[1]] # retain symbol
				return(result)
				}, mc.cores = self$num_cores) # lets try to use less core
			
			spread_dt <- rbindlist(spread_list, use.names = TRUE, fill = TRUE)
			setnames(spread_dt, "index", "date")
			new_cols <- paste0("bidask_", methods_)
			setnames(spread_dt, old = methods_, new = new_cols)
			ohlcv[spread_dt, on = .(symbol, date), bidask_EDGE := as.numeric(i.bidask_EDGE)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_OHL := as.numeric(i.bidask_OHL)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_OHLC := as.numeric(i.bidask_OHLC)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_AR := as.numeric(i.bidask_AR)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_AR2 := as.numeric(i.bidask_AR2)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_CS := as.numeric(i.bidask_CS)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_CS2 := as.numeric(i.bidask_CS2)] # hard coded, no choice: most memory efficient way
			ohlcv[spread_dt, on = .(symbol, date), bidask_ROLL := as.numeric(i.bidask_ROLL)] # hard coded, no choice: most memory efficient way

			rm(spread_dt)
			end_time <- tictoc::toc()
			bid_ask_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Bid Ask Spread: ", bid_ask_time, " Seconds")) # NEED CHANGE THIS
			rm(start_time, bid_ask_dt, symbol_list, methods_, spread_list, new_cols, end_time, bid_ask_time) # need to remove to conserve memory space
			#=================== Price Range Factor ===================
            # Price range of current close price against rolling highest/lowest closing price
            # if it is one, means it is the highest/lowest closing price
			# MIGHT WANT TO ADD MORE WINDOWS/MAKE IT MORE FLEXIBLE
            print("Price range factor...")
            start_time <- tictoc::tic()
			#' @note in the event the symbol has less than 500 rows
			if (nrow(ohlcv) > 500) {
				ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 500)), by = symbol]
				ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 500)), by = symbol]
				ohlcv[, price_range_factor_highest_500_or_300 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]
			} else if (nrow(ohlcv) > 300){
				ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 300)), by = symbol]
				ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 300)), by = symbol]
				ohlcv[, price_range_factor_highest_500_or_300 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]
			} else {
			   ohlcv[, price_range_factor_highest_500_or_300 := NULL]
			}

            ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 252)), by = symbol]
            ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 252)), by = symbol]
            ohlcv[, price_range_factor_252 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]

            ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 125)), by = symbol]
            ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 125)), by = symbol]
            ohlcv[, price_range_factor_125 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]

            ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 66)), by = symbol]
            ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 66)), by = symbol]
            ohlcv[, price_range_factor_66 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]

            ohlcv[, rolling_high := as.numeric(roll::roll_max(x = high, width = 22)), by = symbol]
            ohlcv[, rolling_low := as.numeric(roll::roll_min(x = low, width = 22)), by = symbol]
            ohlcv[, price_range_factor_22 := as.numeric((close - rolling_low) / (rolling_high - rolling_low))]
            ohlcv[, c("rolling_low", "rolling_high") := NULL] # redundant columns
            
			end_time <- tictoc::toc()
            price_range_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Price Range Factor: ", price_range_time, " Seconds"))
            rm(start_time, end_time, price_range_time)
			#=================== Whole Number Discrepency ===================
            # Whole Number Discrepency: price clustering around round numbers
            print("Calculating Whole Number Discrepency...")
			start_time <- tictoc::tic()
            # round the number up to the nearest 0.1 , 1, 100, 1000, 10000
            ohlcv[, pretty_1 := as.numeric(fcase(
                close < 0.1, ceiling(close * 100) / 100,
                close < 1 & close > 0.1, ceiling(close * 10) / 10,
                close < 10 & close >= 1, ceiling(close),
                close < 100 & close >= 10, ceiling(close / 10) * 10,
                close < 1000 & close >= 100, ceiling(close / 100) * 100,
                close < 10000 & close >= 1000, ceiling(close / 1000) * 1000,
                close >= 10000 & close < 100000, ceiling(close / 10000) * 10000,
                close >= 100000, ceiling(close / 100000) * 100000
            ))]
            # round the number down to the nearest 0.1 , 1, 100, 1000, 10000
            ohlcv[, pretty_2 := as.numeric(fcase(
                close < 0.1, floor((close - 0.0001) * 100) / 100,
                close < 1 & close > 0.1, floor((close - 0.0001) * 10) / 10,
                close < 10 & close >= 1, floor((close - 0.0001)),
                close < 100 & close >= 10, floor((close - 0.0001) / 10) * 10,
                close < 1000 & close >= 100, floor((close - 0.0001) / 100) * 100,
                close < 10000 & close >= 1000, floor((close - 0.0001) / 1000) * 1000,
                close >= 10000 & close < 100000, floor((close - 0.0001) / 10000) * 10000,
                close >= 100000, floor((close - 0.0001) / 100000) * 100000
                ))]
            # get the relative distance
            ohlcv[, whole_number_discrepency_ceiling := as.numeric((close - pretty_1) / pretty_1)]
            ohlcv[, whole_number_discrepency_floor := as.numeric((close - pretty_2) / pretty_2)]
			ohlcv[, c("pretty_1", "pretty_2") := NULL] # redundant columns
			
			end_time <- tictoc::toc()
            whole_number_discrepance_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Whole Number Discrepancy: ", whole_number_discrepance_time, " Seconds"))
            rm(start_time, end_time, whole_number_discrepance_time)
			#=================== Rolling Volatility ===================
            # Rolling Volatility: on 1 day return
            print("Calculating Rolling Volatility...")
            start_time <- tictoc::tic()
            new_cols = paste0("sd_", windows_) # windows for volatility: MIGHT WANT TO ADD MORE WINDOWS
            ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(roll::roll_sd(returns_1, width = w))), by = symbol]

            end_time <- tictoc::toc()
            rolling_volatility_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Rolling Volatility: ", rolling_volatility_time, " Seconds"))
            rm(start_time, end_time, rolling_volatility_time)
			#=================== Close-to-Close Volatility ===================
            # Closing Price to Close Price Volatility # 1610 seconds
            print("Calculating OHLC Volatility...")
            start_time <- tictoc::tic()
			windows_ = self$windows # initialize the names of the different windows

            new_cols = paste0("sd_garman.klass_volatility_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(TTR::volatility(cbind(open, high, low, close), n = w, calc = "garman.klass"))),
                    by = symbol]

            new_cols = paste0("sd_parkinson_volatility_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(TTR::volatility(cbind(open, high, low, close), n = w, calc = "parkinson"))),
                    by = symbol]
            
            new_cols = paste0("sd_rogers.satchell_volatility_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) {
                tryCatch({as.numeric(TTR::volatility(cbind(open, high, low, close), n = w, calc = "rogers.satchell"))},
                        error = function(e) NA)
            }), by = symbol]

            new_cols = paste0("sd_gk.yz_volatility_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) {
                tryCatch({as.numeric(TTR::volatility(cbind(open, high, low, close), n = w, calc = "gk.yz"))},
                        error = function(e) NA)
            }), by = symbol]

            new_cols = paste0("sd_yang.zhang_volatility_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) {
                tryCatch({as.numeric(TTR::volatility(cbind(open, high, low, close), n = w, calc = "yang.zhang"))},
                        error = function(e) NA)
            }), by = symbol]

            end_time <- tictoc::toc()
            close_volatility_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Close-to-Close Volatility: ", close_volatility_time, " Seconds"))
            rm(start_time, end_time, close_volatility_time, new_cols)
            #=================== Rolling Skewness ===================
            print("Calculating Rolling Skewness...") # 2640 seconds
            start_time <- tictoc::tic()
            windows_ <- self$windows
            method = c("moment", "fisher", "sample") # can be used to adjust the parameter in PerformanceAnalytics::skewness()
            grid <- base::expand.grid(method = method, window = windows_, stringsAsFactors = FALSE)
            new_cols <- paste0("skew_", grid$method, "_", grid$window)
            
            # Subset to required columns
            rolling_skewness_dt <- ohlcv[, .(symbol, date, close)]
            
            # Split by symbol for parallel processing
            symbol_list <- split(rolling_skewness_dt, by = "symbol", keep.by = TRUE)
            
            # Compute rolling skewness using frollapply in parallel
            rolling_skewness_list <- parallel::mclapply(
                symbol_list,
                function(dt) {
                setorder(dt, date)
                for (w in windows_) {
                    col_name <- paste0("skew_moment_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::skewness(x, method = "moment"), fill = NA, align = "right"))]
                    col_name <- paste0("skew_fisher_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::skewness(x, method = "fisher"), fill = NA, align = "right"))]
                    col_name <- paste0("skew_sample_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::skewness(x, method = "sample"), fill = NA, align = "right"))]
                }
                return(dt)
                },
                mc.cores = self$num_cores
            )
            
            rolling_skewness_dt <- rbindlist(rolling_skewness_list, use.names = TRUE, fill = TRUE)
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_moment_5 := as.numeric(i.skew_moment_5)] # HARD CODED: BEWARE
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_moment_22 := as.numeric(i.skew_moment_22)]
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_fisher_5 := as.numeric(i.skew_fisher_5)]
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_fisher_22 := as.numeric(i.skew_fisher_22)]
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_sample_5 := as.numeric(i.skew_sample_5)]
			ohlcv[rolling_skewness_dt, on = .(symbol, date), skew_sample_22 := as.numeric(i.skew_sample_22)] # HARD CODED: BEWARE

			# for (j in new_cols) { # not memory efficient
			# 	ohlcv[rolling_skewness_dt, on = .(symbol, date), eval(base::substitute(`:=`(col, i.col), list(col = as.name(j))))]
			# 	}

            
            # Timing information
            end_time <- tictoc::toc()
            rolling_skewness_time <- round(end_time$toc - end_time$tic, 3)
            print(paste0("Total Time Taken to Compute Rolling Skewness: ", rolling_skewness_time, " Seconds"))
			rm(start_time, windows_, method, grid, new_cols, rolling_skewness_dt, symbol_list, rolling_skewness_list, end_time, rolling_skewness_time)
			#=================== Rolling Kurtosis ===================
			print("Calculating Rolling Kurtosis...")
            start_time <- tictoc::tic()
            windows_ <- self$windows
			method = c("excess", "moment", "fisher", "sample", "sample_excess") # can be used to adjust the parameter in PerformanceAnalytics::skewness()
            grid <- base::expand.grid(method = method, window = windows_, stringsAsFactors = FALSE)
            new_cols <- paste0("kurtosis_", grid$method, "_", grid$window)

			# Subset to required columns
            rolling_kurtosis_dt <- ohlcv[, .(symbol, date, close)]
            
            # Split by symbol for parallel processing
            symbol_list <- split(rolling_kurtosis_dt, by = "symbol", keep.by = TRUE)
			
			# Compute rolling skewness using frollapply in parallel
            rolling_kurtosis_list <- parallel::mclapply(
                symbol_list,
                function(dt) {
                setorder(dt, date)
                for (w in windows_) {
                    col_name <- paste0("kurtosis_excess_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::kurtosis(x, method = "excess"), fill = NA, align = "right"))]
                    col_name <- paste0("kurtosis_moment_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::kurtosis(x, method = "moment"), fill = NA, align = "right"))]
                    col_name <- paste0("kurtosis_fisher_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::kurtosis(x, method = "fisher"), fill = NA, align = "right"))]
                    col_name <- paste0("kurtosis_sample_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::kurtosis(x, method = "sample"), fill = NA, align = "right"))]
                    col_name <- paste0("kurtosis_sample_excess_", w)
                    dt[, (col_name) := as.numeric(frollapply(close, n = w, FUN = function(x) PerformanceAnalytics::kurtosis(x, method = "sample_excess"), fill = NA, align = "right"))]
                }
                return(dt)
                },
                mc.cores = self$num_cores
            )
            
            # Combine result
            rolling_kurtosis_dt <- rbindlist(rolling_kurtosis_list, use.names = TRUE, fill = TRUE)
            
            # Merge back into original ohlcv table
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_excess_5 := as.numeric(i.kurtosis_excess_5)] # HARD CODED: BEWARE
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_excess_22 := as.numeric(i.kurtosis_excess_22)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_moment_5 := as.numeric(i.kurtosis_moment_5)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_moment_22 := as.numeric(i.kurtosis_moment_22)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_fisher_5 := as.numeric(i.kurtosis_fisher_5)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_fisher_22 := as.numeric(i.kurtosis_fisher_22)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_sample_5 := as.numeric(i.kurtosis_sample_5)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_sample_22 := as.numeric(i.kurtosis_sample_22)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_sample_excess_5 := as.numeric(i.kurtosis_sample_excess_5)]
			ohlcv[rolling_kurtosis_dt, on = .(symbol, date), kurtosis_sample_excess_22 := as.numeric(i.kurtosis_sample_excess_22)] # HARD CODED: BEWARE
            
            # Timing information
            end_time <- tictoc::toc()
            rolling_kurtosis_time <- round(end_time$toc - end_time$tic, 3)
            print(paste0("Total Time Taken to Compute Rolling Kurtosis: ", rolling_kurtosis_time, " Seconds"))
			rm(start_time, method, grid, new_cols, rolling_kurtosis_dt, symbol_list, rolling_kurtosis_list,
				end_time, rolling_kurtosis_time)
            
			#=================== Return Output ===================
			num_cols <- setdiff(names(ohlcv), c("symbol", "date"))
			data.table::setkey(ohlcv, symbol, date)
			return(ohlcv)
		},
		#=========================================== FOURTH FUNCTION ===========================================
		prepare_technical_indicators = function(symbol_df){
			# @toupdate: ADD MORE WINDOWS to some indicators such as SMA
			checkmate::assert_data_table(symbol_df)
			checkmate::assert_true(symbol_df[, .N, by = symbol][, all(N > max(self$windows))]) # ensures that all symbols have rows that are > window size
			checkmate::test_subset(c("symbol","open","high","low","close"), colnames(symbol_df)) # check if all the necessary columns are there
			
			ohlcv <- data.table::copy(symbol_df) # creates a deep copy to prevent changes to new data.table() from affecting the original
			#setkey(ohlcv, c("symbol")) # set the key to be "symbol" which is the ticker and date
			windows_ = self$windows # initialize the names of the different windows
			
            # plan(multisession) 
			data.table::setDTthreads(threads = 0, throttle = 1) # for parallel processing: 0 means use all logical CPUs available
			if(is.null(self$at)){
				at_ = 1:nrow(ohlcv)} # row index which is used to calculate indicators
				else{
				at_ = self$at}

			setorder(ohlcv, symbol, date)
			ids = c("date", "symbol")
			start_cols = ohlcv[, colnames(.SD)] # the original ohlcv columns

			print("Preparing Basic Predictors")
			#=================== Technical Indicator 1: Average True Range =================== 6.978 sec
			print("Calculating technical indicators (Average True Range [ATR])...")
            start_time <- tictoc::tic()
      		new_cols <- expand.grid("atr", c("tr", "atr", "trueHigh", "trueLow"), 14) # window: 14
      		new_cols <- paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
      		ohlcv[, (new_cols) := do.call(cbind, lapply(14, function(w) as.data.frame(TTR::ATR(cbind(high, low, close), n = w)))), by
      		= symbol]
      		      		
      		new_new_cols = paste0(new_cols, "_closedv")
      		ohlcv[, (new_new_cols) := lapply(.SD, function(x) as.numeric(x / close)), .SDcols = new_cols] # normalising the values
      		
            end_time <- tictoc::toc()
            atr_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute ATR: ", atr_time, " Seconds"))
            rm(start_time, end_time, atr_time, new_cols)
			#=================== Technical Indicator 2: Bollinger Bands =================== 13.27 sec
			print("Calculating technical indicators (Bollinger Bands)...")
            start_time <- tictoc::tic()
      		new_cols <- expand.grid("bbands", c("dn", "mavg", "up", "pctB"), 14) # window: 14
      		new_cols <- paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
      		
			ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::BBands(close, n = w)))), by = symbol]
      		new_cols_change <- new_cols[grep("bbands.*up|bbands.*mavg|bbands.*dn", new_cols)] # Filters out the up, mavg, and dn bands (excludes pctB)
      		ohlcv[, (new_cols_change) := lapply(.SD, function(x) as.numeric(close / x)), .SDcols = new_cols_change] # normalise the values using close price
      		
            end_time <- tictoc::toc()
            atr_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute ATR: ", atr_time, " Seconds"))
            rm(start_time, end_time, atr_time, new_cols)
			
			#=================== Technical Indicator 3: Chaikin Accumulation/Distribution (AD) =================== 4.48 sec
			print("Calculating technical indicators (Chaikin Accumulation/Distribution)...")
            start_time <- tictoc::tic()
			ohlcv[, ("chaikinad_one_window") := as.numeric(TTR::chaikinAD(cbind(high,low,close), volume)), by = symbol]
      		
            end_time <- tictoc::toc()
            chaikin_ad_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Chaikin Accumulation/Distribution: ", chaikin_ad_time, " Seconds"))
            rm(start_time, end_time, chaikin_ad_time)
			
			#=================== Technical Indicator 4: Chaikin Volatility =================== 3.578 sec
			print("Calculating technical indicators (Chaikin Volatility)...")
            start_time <- tictoc::tic()
			ohlcv[, ("chaikinvolatility_one_window") := as.numeric(TTR::chaikinVolatility(cbind(high,low), n = 10)), by = symbol]
      		
            end_time <- tictoc::toc()
            chaikin_volatility <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Chaikin Volatility: ", chaikin_volatility, " Seconds"))
            rm(start_time, end_time, chaikin_volatility)
			
			#=================== Technical Indicator 5: Close Location Value (CLV) =================== 2.114 sec
      		print("Calculating Close Location Value (CLV)...")
			start_time <- tictoc::tic()
      		ohlcv[, ("clv_one_window") := as.numeric(TTR::CLV(cbind(high, low, close))), by = symbol]
			end_time <- tictoc::toc()
            clv <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Close Location Value (CLV): ", clv, " Seconds"))
            rm(start_time, end_time, clv)
			
			#=================== Technical Indicator 6: Chaikin Money Flow (CMF) =================== 14.842 sec 
      		print("Calculating Chaikin Money Flow (CMF)...")
			start_time <- tictoc::tic()
			new_cols = paste0("cmf_", windows_)
			ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(TTR::CMF(cbind(high, low, close), volume, n = w))), by = symbol]
			end_time <- tictoc::toc()
            cmf <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Chaikin Money Flow (CMF): ", cmf, " Seconds"))
            rm(start_time, end_time, cmf, new_cols)
			
			#=================== Technical Indicator 7: Chande Momentum Oscillator (CMO) =================== 25.27 sec
      		print("Calculating Chande Momentum Oscillator (CMO)...")
			start_time <- tictoc::tic()
			new_cols = paste0("cmo_", windows_)
			ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(TTR::CMO(close, n = w))), by = symbol]
			new_cols = paste0("cmo_volume_", windows_)
			ohlcv[, (new_cols) := lapply(windows_, function(w) as.numeric(TTR::CMO(volume, n = w))), by = symbol]
			end_time <- tictoc::toc()
            cmo <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Chande Momentum Oscillator (CMO): ", cmo, " Seconds"))
            rm(start_time, end_time, cmo, new_cols)
			
			#=================== Technical Indicator 8: Donchian Channel =================== 9.627 Sec
			print("Calculating technical indicators (Donchian Channel)...")
            start_time <- tictoc::tic()
            new_cols = expand.grid("dochian", c("high", "mid", "low"), windows_)
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")

			ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::DonchianChannel(cbind(high, low), n = w)))),
				by = symbol]
			
			ohlcv[, (new_cols) := lapply(.SD, function(x) close / x), .SDcols = new_cols]
			
            end_time <- tictoc::toc()
            donchian_channel_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Donchian Channel ", donchian_channel_time, " Seconds"))
            rm(start_time, end_time, donchian_channel_time, new_cols)
            

			#=================== Technical Indicator 9: DV Intermediate Oscillator (DVI)=================== 29.227
            print("Calculating technical indicators (DV Intermediate Oscillator (DVI))...")
			start_time <- tictoc::tic()
      		new_cols = expand.grid("dvi", c("dvi_mag", "dvi_str", "dvi"), windows_)
      		new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
      		ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::DVI(close, n = w)))),
      			by = symbol]

			end_time <- tictoc::toc()
            dvi_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute DV Intermediate Oscillator (DVI) ", dvi_time, " Seconds"))
            rm(start_time, end_time, dvi_time, new_cols)
            

			#=================== Technical Indicator 10: Guppy Multiple Moving Average (GMMA)=================== 17.163 Sec
			print("Calculating technical indicators (Guppy Multiple Moving Average (GMMA))...")
			start_time <- tictoc::tic()
			new_cols = gsub(" ", "_", colnames(TTR::GMMA(ohlcv[1:252, close])))
			new_cols = expand.grid("GMMA", new_cols)
			new_cols <- paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
			ohlcv[, (new_cols) := as.data.frame(TTR::GMMA(close)), by = symbol]
			ohlcv[, (new_cols) := lapply(.SD, function(x) close / x), .SDcols = new_cols]

			end_time <- tictoc::toc()
            gmma_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Guppy Multiple Moving Average (GMMA) ", gmma_time, " Seconds"))
            rm(start_time, end_time, gmma_time, new_cols)
            
			#=================== Technical Indicator 11: Keltner Channels =================== 210.677 Sec
			print("Calculating technical indicators (Keltner Channels)...")
			start_time <- tictoc::tic()
			new_cols = expand.grid("keltnerchannels", c("dn", "mavg", "up"), windows_)
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
            
            ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::keltnerChannels(cbind(high, low, close), n = w)))), by = symbol]
            new_cols_change <- new_cols[grep("keltnerchannels.*up|keltnerchannels.*mavg|keltnerchannels.*dn", new_cols)]
            ohlcv[, (new_cols_change) := lapply(.SD, function(x) close / x), .SDcols = new_cols_change]

			end_time <- tictoc::toc()
            kc_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Keltner Channels ", kc_time, " Seconds"))
            rm(start_time, end_time, kc_time, new_cols, new_cols_change)
            
            #=================== Technical Indicator 12: Know Sure Thing Indicator (KST) =================== 10.769 Sec
			print("Calculating technical indicators Know Sure Thing Indicator (KST)...")
			start_time <- tictoc::tic()
            new_cols = expand.grid("kst", c("kst", "signal"), windows_)
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
            ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::KST(close, n = ceiling(w / 2), nROC = w)))),
                by = symbol]

			end_time <- tictoc::toc()
            kst_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Know Sure Thing Indicator (KST) ", kst_time, " Seconds"))
            rm(start_time, end_time, kst_time, new_cols)
            
            #=================== Technical Indicator 13: Money Flow Index (MFI) =================== 205.876 Sec
            print("Calculating technical indicators Money Flow Index (MFI)...")
			start_time <- tictoc::tic()
            new_cols = paste0("mfi_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::MFI(cbind(high, low, close), volume, n = w)), by = symbol]

            end_time <- tictoc::toc()
            mfi_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Money Flow Index (MFI) ", mfi_time, " Seconds"))
            rm(start_time, end_time, mfi_time, new_cols)
            
            #=================== Technical Indicator 14: Relative Strength Index (RSI) =================== 10.406 Sec
            print("Calculating technical indicators Relative Strength Index (RSI)...")
			start_time <- tictoc::tic()
            new_cols = paste0("rsi_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::RSI(close, n = w)), by = symbol]

            end_time <- tictoc::toc()
            rsi_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Relative Strength Index (RSI) ", rsi_time, " Seconds"))
            rm(start_time, end_time, rsi_time, new_cols)
            
            #=================== Technical Indicator 15: Welles Wilder’s Directional Movement Index (ADX) =================== 38.153 Sec
            print("Calculating technical indicators Welles Wilder’s Directional Movement Index (ADX)...")
			start_time <- tictoc::tic()
            new_cols = expand.grid("adx", c("dip", "din", "dx", "adx"), 14) # specifically used 14, else got error
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
            ohlcv[, (new_cols) := do.call(cbind, lapply(14, function(w) as.data.frame(TTR::ADX(cbind(high, low, close), n = w)))), by = symbol]

            new_cols = expand.grid("adx", c("dip", "din", "dx", "adx"), 22 * 3)
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
            ohlcv[, (new_cols) := do.call(cbind, lapply(22 * 3, function(w) as.data.frame(TTR::ADX(cbind(high, low, close), n = w)))),
                    by = symbol]

            end_time <- tictoc::toc()
            adx_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Welles Wilder’s Directional Movement Index (ADX) ", adx_time, " Seconds"))
            rm(start_time, end_time, adx_time, new_cols)
            
            #=================== Technical Indicator 16: Commodity Channel Index (CCI) =================== 198.35 Sec
            print("Calculating technical indicators Commodity Channel Index (CCI)...")
			start_time <- tictoc::tic()
            new_cols = paste0("cci_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::CCI(cbind(high, low, close), n = w)), by = symbol]

            end_time <- tictoc::toc()
            cci_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Commodity Channel Index (CCI) ", cci_time, " Seconds"))
            rm(start_time, end_time, cci_time, new_cols)
            
            #=================== Technical Indicator 17: On Balance Volume (OBV) ===================
            print("Calculating technical indicators On Balance Volume (OBV)...")
			start_time <- tictoc::tic()
            ohlcv[, ("obv") := TTR::OBV(close, volume), by = symbol]

            end_time <- tictoc::toc()
            obv_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute On Balance Volume (OBV) ", obv_time, " Seconds"))
            rm(start_time, end_time, obv_time)
            
            #=================== Technical Indicator 18: Parabolic Stop-and-Reverse (SAR) ===================
            print("Calculating technical indicators Parabolic Stop-and-Reverse (SAR)...")
			start_time <- tictoc::tic()
            ohlcv[, ("sar") := as.vector(TTR::SAR(cbind(high, close))), by = symbol]
            ohlcv[, ("sar") := close / sar] # normalize the relationship between closing and parabolic SAR value

            end_time <- tictoc::toc()
            obv_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Parabolic Stop-and-Reverse (SAR) ", obv_time, " Seconds"))
            rm(start_time, end_time, obv_time)
            
            #=================== Technical Indicator 19: William’s %R (WPR) ===================
            print("Calculating technical indicators William’s %R (WPR)...")
			start_time <- tictoc::tic()
            new_cols = paste0("wpr_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::WPR(cbind(high, low, close), n = w)), by = symbol]

            end_time <- tictoc::toc()
            wpr_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute William’s %R (WPR) ", wpr_time, " Seconds"))
            rm(start_time, end_time, wpr_time)
            
            #=================== Technical Indicator 20: Aroon Indicator ===================
            print("Calculating technical indicators Aroon Indicator...")
			start_time <- tictoc::tic()
            new_cols = expand.grid("aroon", c("aroonUp", "aroonDn", "oscillator"), windows_)
            new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")
            ohlcv[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::aroon(cbind(high, low), n = w)))), by = symbol]

            end_time <- tictoc::toc()
            aroon_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Aroon Indicator ", aroon_time, " Seconds"))
            rm(start_time, end_time, aroon_time, new_cols)
            
            #=================== Technical Indicator 21: Percent Rank Indicator ===================
            print("Calculating technical indicators Percent Rank Indicator...")
			start_time <- tictoc::tic()
            new_cols = paste0("percent_rank_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) QuantTools::roll_percent_rank(close, n = w)), by = symbol]

            end_time <- tictoc::toc()
            pct_rank_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Percent Rank Indicator ", pct_rank_time, " Seconds"))
            rm(start_time, end_time, pct_rank_time)
            
			#=================== Technical Indicator 22: Moving Average Convergence Divergence (MACD) ===================
			print("Calculating technical indicators (Moving Average Convergence Divergence (MACD))...")
            start_time <- tictoc::tic()
      		new_cols <- expand.grid("macd","close", c("macd", "signal")) # window: 14
            new_cols <- paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")

			# MACD: closing price
            ohlcv[, (new_cols) := {
                out <- TTR::MACD(.SD[, close], maType = "EMA")
                as.data.table(out[, c("macd", "signal")])
                }, by = symbol]

            new_cols <- expand.grid("macd","volume", c("macd", "signal")) # window: 14
            new_cols <- paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")

			# MACD: volume
            ohlcv[, (new_cols) := {
                out <- TTR::MACD(.SD[, volume], maType = "EMA")
                as.data.table(out[, c("macd", "signal")])
                }, by = symbol]
      		
            end_time <- tictoc::toc()
            macd_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute MACD: ", macd_time, " Seconds"))
            rm(start_time, end_time, macd_time, new_cols)
			
			#=================== Technical Indicator 23: Moving Averages ===================
			print("Calculating technical indicators (Moving Averages)...")
            start_time <- tictoc::tic()
      		ohlcv[, "SMA_10" := TTR::SMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "EMA_10" := TTR::EMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "DEMA_10" := TTR::DEMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "WMA_10" := TTR::WMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "ZLEMA_10" := TTR::ZLEMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "VWAP_10" := TTR::VWAP(ohlcv[,"close"], ohlcv[,"volume"], n = 10)]
            ohlcv[, "HMA_10" := TTR::HMA(ohlcv[,"close"], n = 10)]
            ohlcv[, "ALMA_10" := TTR::ALMA(ohlcv[,"close"], n = 10)]
      		
            end_time <- tictoc::toc()
            ma_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Moving Average: ", ma_time, " Seconds"))
            rm(start_time, end_time, ma_time)
			
			#=================== Technical Indicator 24: Signal to Noise Ratio ===================
            print("Calculating technical indicators Signal to Noise Ratio (SNR)...")
            start_time <- tictoc::tic()
            new_cols = paste0("SNR_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::SNR(cbind(high, low, close), n = w)), by = symbol]
      		
            end_time <- tictoc::toc()
            snr_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Signal to Noise Ratio (SNR): ", snr_time, " Seconds"))
            rm(start_time, end_time, snr_time)
			
			#=================== Technical Indicator 25: Triple Smoothed Exponential Oscillator ===================
            print("Calculating technical indicators (Triple Smoothed Exponential Oscillator)...")
            start_time <- tictoc::tic()
            ohlcv[, c("TRIX_20", "TRIX_signal_20") := {
                out <- TTR::TRIX(close, n = 20)
                as.data.table(out)}, by = symbol]
                            
            end_time <- tictoc::toc()
            trix_time <- round(end_time$toc - end_time$tic,3)
			print(paste0("Total Time Taken to Compute Triple Smoothed Exponential Oscillator: ", trix_time, " Seconds"))
            rm(start_time, end_time, trix_time)
			
			#=================== Technical Indicator 26: Vertical Horizontal Filter ===================
            print("Calculating technical indicators Vertical Horizontal Filter...")
			start_time <- tictoc::tic()
            new_cols = paste0("vertical_horizontal_filter_", windows_)
            ohlcv[, (new_cols) := lapply(windows_, function(w) TTR::VHF(close, n = w)), by = symbol]

            end_time <- tictoc::toc()
            vhf_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Compute Vertical Horizontal Filter ", vhf_time, " Seconds"))
            rm(start_time, end_time, vhf_time)
			#=================== Return Output ===================
			data.table::setkey(ohlcv, symbol, date)
			return(ohlcv)
		}
	)
		
)



# #======= TEST =======
# current_path = getwd()
# feature_loader <- technical_indicators$new() # new instance
# df <- feature_loader$load_data("/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate", "stocks_daily", "stocks_daily") # load data in
# aapl_df <- df[symbol == "aapl"]

# prepare_df <- feature_loader$prepare_data(aapl_df) # basic indicators
# prepare_technical_indicators <- feature_loader$prepare_technical_indicators(aapl_df) # technical indicators


# tail(prepare_technical_indicators)

# # prepare_df %>% filter(symbol== 'aapl') %>% select(date,symbol,volume_conc_2_5,volume_conc_22_44)

# # head(prepare_df)
# plot_diagram_company(prepare_df, "aapl", "volume_conc_2_5")

# tail(prepare_df)

# prepare_df %>% filter(symbol == "aapl")



# df2 <- data.table::copy(df)
# # df2 <- df2[, .SD, .SDcols = c("symbol", "date", "high", "low")]
# # windows_ <- c(5,22)
# # # print("Calculating technical indicators (Donchian Channel)...")
# # # start_time <- tictoc::tic()
# # new_cols = expand.grid("dochian", c("high", "mid", "low"), windows_)
# # new_cols = paste(new_cols$Var1, new_cols$Var2, new_cols$Var3, sep = "_")

# # df2[, (new_cols) := do.call(cbind, lapply(windows_, function(w) as.data.frame(TTR::DonchianChannel(cbind(high, low), n = w)))),
# #             by = symbol]
# # df2[, (new_cols) := lapply(.SD, function(x) close / x), .SDcols = new_cols]


# sma_short = QuantTools::sma( df2$close, 50 )
# sma_long  = QuantTools::sma( df2$close, 100 )
# crossover = QuantTools::crossover( sma_short, sma_long ) # output will be NA, UP, DN
