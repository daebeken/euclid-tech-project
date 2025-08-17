# [4] TIME SERIES MODEL: Ufrisk: computing the value at risk (VaR) and expected shortfall (ES) using parametric and semiparametric GARCH-type models.
# 7: ROLLINGUfrisk IS OK
# https://cran.r-project.org/web/packages/ufRisk/index.html
# @description: Enables the user to calculate Value at Risk (VaR) and Expected Shortfall (ES) by means of various parametric and semiparametric GARCH-type models.
# @description: Ufrisk has many different types of GARCH models that can be used to compute Value at Risk and Expected Shortfall
# @params: models: "sGARCH", "lGARCH", "eGARCH", "apARCH", "fiGARCH", "filGARCH"
# @params: distr = c("norm", "std")
# compute on the close: if i use returns, it will give an error: ugarchfilter-->error: parameters names do not match specification; Expected Parameters are: omega alpha1 beta1

#================================================================= MAIN CLASS ======================================================================================================
# create R6 class
feature_five <- R6::R6Class(
	"feature_five",
	# initialize the class
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch", "quarks", "ufRisk")
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
        #=========================================== FUNCTION WITH THE MAIN ALGORITHM ===============================================
        rolling_function = function(x, window, model, distr, price_col){
            # @description: the main algorithm for Quarks
            # @params: x is the dataframe, containing date, symbol and ohlcv
            # @params: window: size of the rolling window
            # @params: model: the model to be used: can be c("sGARCH", "lGARCH", "eGARCH", "apARCH", "fiGARCH", "filGARCH")
            # @params: distr: the distr to be used: can be c("norm", "std")
            # @returns: Value at Risk (VaR) and Expected Shortfall (ES), normalised to future return
            
            # in rolling_function function "es", "var_e", "var_v
            template_cols <- c(paste0("es_", model, "_", distr, "_", window), paste0("var_e_", model, "_", distr, "_", window), paste0("var_v_", model, "_", distr, "_", window))
            empty_row <- data.table::as.data.table(
                setNames(as.list(rep(NA_real_, length(template_cols))), template_cols)
            )

            # compute returns
            if (length(unique(x$symbol)) > 1) return(empty_row)

            # main algorithm, dependent on method
            y <- tryCatch(base::suppressMessages(
                ufRisk::varcast(
                    x[[price_col]], 
                    a.v = 0.99, # confidence level to compute VaR
                    a.e = 0.975, # confidence level to computr ES
                    model = model,
                    distr = distr,
                    smooth = "none",
                    n.out = 1)), error = function(e) NULL)
                if (is.null(y)) return (empty_row)
                
            es = y$ES # expected shortfall at 1% quantile (a.e)
            var_e = y$VaR.e # value at risk at 2.5% quantile (a.e)
            var_v = y$VaR.v # value at risk at 1% quantile (a.v)

            results <- data.table::as.data.table(
                t(setNames(
                    c(es, var_e, var_v),
                    template_cols
                ))
            )
            return(results[])
            },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, rolling_function, model, distr){
            # @description: uses the mirai package to integrate asynchronous parallel programming into the script
            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional
            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col, model, distr) {
                    data.table::setDTthreads(2)  # Allow light multithreading inside workers
                    out <- lapply(index_chunk, function(i) {
                        window_data <- dt[(i - windows_ + 1):i]
                        rolling_function(window_data, windows_, model, distr, price_col)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(
                    dt = subset_df,
                    windows_ = windows_,
                    model = model,
                    distr = distr,
                    price_col = price_col,
                    rolling_function = rolling_function
                )
            )[]
            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("ufrisk_", stat_names[j])
                result_dt[indices, (colname) := res_matrix[[j]]]
            }

            result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
            new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
            subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
            # merged <- merge(subset_df, result_dt, by = c("symbol", "date"), all.x = TRUE)
            return(subset_df)
        },
        #=========================================== CONSOLIDATION CODE ===============================================
        other_finfeatures_indicators = function(symbol_df) {
            # @description: consolidating all functions together to run as 1 function
            ohlcv <- data.table::copy(symbol_df)
            data.table::setkey(ohlcv, symbol, date) # added this in 020825
            data.table::setDTthreads(threads = 0, throttle = 1)
            setorder(ohlcv, symbol, date)
            
            print("=================== COMPUTING VaR and Expected Shortfall from Ufrisk Package ===================")
            #=================== OTHER INDICATORS 7: VaR and Expected Shortfall from Ufrisk Package ===================
            print("[7] Calculating VaR and Expected Shortfall from Ufrisk Package...")
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
            model <- "sGARCH" #, "lGARCH", "filGARCH", "apARCH", "fiGARCH", "eGARCH") # all available models in ufrisk
            distr <- "norm" #, "std") # all available distributions in ufrisk
            # grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
            # ======================================= the code to be runned =======================================
            # focused on fhs because it is what the author encourages
            # try_list <- lapply(smaller_symbol_list, function(dt){
            #             self$run_code(dt, windows_, price_col, self$rolling_function, model = "sGARCH", distr = "norm")})
            
            # # to have more than one
            # try_list2 <- lapply(smaller_symbol_list, function(dt){
            #             self$run_code(dt, windows_, price_col, self$rolling_function, model = "EWMA", method = "fhs")})

            try_list1 <- as.data.table(self$run_code(symbol_df, windows_, price_col, self$rolling_function, model = "sGARCH", distr = "norm"))
            # try_list2 <- as.data.table(self$run_code(symbol_df, windows_, price_col, self$rolling_function, model = "EWMA", distr = "fhs")) # to have more than one
            # results_dt <- data.table::rbindlist(list(try_list1,try_list2), use.names = TRUE, fill = TRUE)
            
            results_dt <- try_list1
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            ufrisk_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Calculating VaR and Expected Shortfall from Ufrisk Package ", ufrisk_time, " Seconds"))
            rm(start_time, windows_, price_col, model, distr, try_list1, results_dt, new_cols, end_time, ufrisk_time)
            mirai::daemons(0) # stop the active daemon process cleanly and clean up background workers
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


# # ================================================================ DEBUGGING ================================================================
# windows_ = 252
# aapl_df <- df["aapl"]
# n_rows <- nrow(aapl_df)
# indices <- (windows_ + 1):n_rows
# result_dt <- data.table(date = aapl_df$date)

# subset_dt <- aapl_df[(indices[1] - windows_ + 1):indices[1]]

# feature_loader$rolling_function(subset_dt, windows_, "sGARCH", "norm") # rolling function works

# i = indices[1]

# feature_loader$mirai_function(i,aapl_df,windows_,feature_loader$rolling_function, "sGARCH", "norm") # mirai function works too

# feature_loader$run_code(aapl_df, windows_, "close", feature_loader$rolling_function, "sGARCH", "norm" ) # run_code function works now



# results <- mirai::mirai_map(
#     .x = indices,
#     .f = feature_loader$mirai_function, # need add a self$ in front
#     .args = list(
#         dt = aapl_df,
#         windows_ = windows_,
#         model = "sGARCH",
#         distr = "norm",
#         rolling_function = feature_loader$rolling_function
#     )
# )[]


# # look at other_finfeatures_indicators function now
# windows_ = 252 # one year
# price_col = "close"
# model <- "sGARCH" #, "lGARCH", "filGARCH", "apARCH", "fiGARCH", "eGARCH") # all available models in ufrisk
# distr <- "norm" #, "std") # all available distributions in ufrisk

# symbol_list <- split(aapl_df, by = "symbol", keep.by = TRUE)
# smaller_symbol_list <- symbol_list[1] # try on 1 symbol first: REMOVE IF WANT TO RUN THE FULL CODE

# try_list <- lapply(smaller_symbol_list, function(dt){
#     feature_loader$run_code(dt, windows_, price_col, feature_loader$rolling_function, model = "sGARCH", distr = "norm")})

# results_dt <- data.table::rbindlist(try_list, use.names = TRUE, fill = TRUE)
# data.table::setkey(results_dt, symbol, date)
# new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
# merged <- merge(aapl_df, results_dt, by = c("symbol", "date"), all.x = TRUE)
# # ================================================================ END OF CODE, TEST NOW ================================================================
