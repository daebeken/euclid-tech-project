# [9] TIME SERIES MODEL: FRACTIONAL DIFFERENCED ARIMA(p,d,q) MODEL
# [11] ROLLINGFRACDIFF IS OK
# https://cran.r-project.org/web/packages/fracdiff/index.html

# @description: Maximum likelihood estimation of the parameters of a fractionally differenced ARIMA(p,d,q) model
# @description: computes using CLOSING price
# @description: calculates coefficients of ARFIMA model from fracdiff package

#======= Main Class =======
# create R6 class
feature_nine <- R6::R6Class(
	"feature_nine",
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch", "quarks", "ufRisk", "theftdlc", "theft", "tsfeatures", "tsDyn", "fracdiff", "callr")
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
        rolling_function = function(x, window, price_col, params_arifma, params_d){
        # @description: the main algorithm for TsDyn
        # @params: x is the dataframe, containing date, symbol and ohlcv
        # @params: window: size of the rolling window
        # @params: price_col: the price column to be used: OHLCV
        # @params: params_arifma, params_d: these are EXPANDED GRIDS assigned as global variable to improve efficiency in the code
        # @returns: values given by that time series method
            # main algorithm, dependent on method

            # compute arifma coefficients
            fitted <- lapply(seq_along(params_arifma$nar), function(i) {
                fracdiff::fracdiff(x[, get(price_col)], nar = params_arifma$nar[i], nma = params_arifma$nma[i])})
            
            fitted_coefs <- lapply(fitted, coef)
            
            for (i in seq_along(fitted_coefs)) {
                names(fitted_coefs[[i]]) <- paste0(names(fitted_coefs[[i]]), "_", i)}
            vars <- do.call(c, fitted_coefs)

            # compute d (integrative coefficient in ARIMA)
            ds <- lapply(params_d$bandw_exp, function(be) {
                # iterate over every bandw_exp
                fdGPH_ <- as.data.frame(fracdiff::fdGPH(x[, get(price_col)], bandw.exp = be))
                    colnames(fdGPH_) <- paste0(colnames(fdGPH_), "_fdGPH_", be)
                fdSperio_ <- as.data.frame(fracdiff::fdSperio(x[, get(price_col)], bandw.exp = be, beta = 0.9))
                    colnames(fdSperio_) <- paste0(colnames(fdSperio_), "_fdSperio_", be)
                    cbind(fdGPH_, fdSperio_)
                })

            vars_ds <- unlist(ds)
            names(vars_ds) <- gsub("//.", "_", names(vars_ds))
            
            # final output
            output <- c(vars, vars_ds)
            names(output) <- paste0(names(output), "_", window)
            output <- data.table(t(output))

            # Final result
            return(output)
        },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, rolling_function, params_arifma, params_d){
            # @description: uses the mirai package to integrate asynchronous parallel programming into the script

            # subset_df[, returns := get(price_col) / data.table::shift(get(price_col)) - 1][!is.na(returns)] # compute returns, removes first row

            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional

            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col, params_arifma, params_d) {
                    data.table::setDTthreads(2)  # Allow light multithreading inside workers
                    out <- lapply(index_chunk, function(i) {
                        window_data <- dt[(i - windows_ + 1):i]
                        rolling_function(window_data, windows_, price_col, params_arifma, params_d)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(
                    dt = subset_df,
                    windows_ = windows_,
                    rolling_function = rolling_function,
                    price_col = price_col,
                    params_arifma = params_arifma, 
                    params_d = params_d
                )
            )[]

            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("frac_diff_", stat_names[j])
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
            
            print("=================== [11] COMPUTING time series features from fracdiff Package ===================")
            #=================== OTHER INDICATORS 11: VaR and Expected Shortfall from fracdiff Package ===================
            print("[11] Calculating time series features from fracdiff Package...")
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
            
            # parameters for arifma
            nar = c(1,2) # how many autoregressive parameters p
            nma = c(1,2) # how many moving average parameters q
            bandw_exp = c(0.3, 0.6, 0.9) # for fdGPH and fdSperio # 0.1: too low - will give NaN/Inf/NA
            
            params_arifma <- expand.grid(
                nar = nar,
                nma = nma,
                stringsAsFactors = FALSE)

            colnames(params_arifma) <- c("nar", "nma")
            
            # parameters for computing d
            params_d <- expand.grid(
                nar = nar,
                nma = nma,
                bandw_exp = bandw_exp)
            colnames(params_d) <- c("nar", "nma", "bandw_exp")
            # ======================================= the code to be runned =======================================
            # focused on fhs because it is what the author encourages
            try_list <- self$run_code(symbol_df, windows_, price_col, self$rolling_function, params_arifma, params_d)
            results_dt <- try_list
            # results_dt <- data.table::rbindlist(c(try_list,try_list2), use.names = TRUE, fill = TRUE)
            # results_dt <- data.table::rbindlist(try_list, use.names = TRUE, fill = TRUE)
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            frac_diff_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Calculating time series features from fracdiff Package ", frac_diff_time, " Seconds"))
            rm(start_time, windows_, price_col, nar, nma, bandw_exp, params_arifma, params_d, try_list, results_dt, new_cols, end_time, frac_diff_time)
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
# prepare_df <- feature_loader$other_finfeatures_indicators(df) #test2: 403 seconds for 2 ticker

# tail(prepare_df["a"])
# # tail(prepare_df["aa"])

# # ================================================================ END OF CODE, TEST NOW ================================================================
# # # start_time <- tictoc::tic()
# # tsfeature_dt <- data.table::copy(df)
# # tsfeature_dt <- tsfeature_dt["aapl"]
# # n_rows <- nrow(tsfeature_dt)
# # window <- 252 # size of rolling window

# # # nar = 1
# # nar = c(1,2) # how many autoregressive parameters p

# # # nma = 1
# # nma = c(1,2) # how many moving average parameters q

# # # bandw_exp = 0.1 # for fdGPH and fdSperio
# # bandw_exp = c(0.1, 0.5, 0.9)



# # # compute arifma coefficients
# # params_arifma <- expand.grid(
# #     nar = nar,
# #     nma = nma,
# #     stringsAsFactors = FALSE)

# # colnames(params_arifma) <- c("nar", "nma")


# # fitted <- lapply(seq_along(params_arifma$nar), function(i) {
# #     fracdiff::fracdiff(tsfeature_dt[, close], nar = params_arifma$nar[i], nma = params_arifma$nma[i])
# # })

# # fitted_coefs <- lapply(fitted, coef)

# # for (i in seq_along(fitted_coefs)) {
# #     names(fitted_coefs[[i]]) <- paste0(names(fitted_coefs[[i]]), "_", i)
# # }

# # vars <- do.call(c, fitted_coefs)

# # # compute d (integrative coefficient in ARIMA)
# # params_d <- expand.grid(
# #     nar = nar,
# #     nma = nma,
# #     bandw_exp = bandw_exp)
# # colnames(params_d) <- c("nar", "nma", "bandw_exp")

# # ds <- lapply(params_d$bandw_exp, function(be) {
# #     # iterate over every bandw_exp
# #     fdGPH_ <- as.data.frame(fracdiff::fdGPH(tsfeature_dt[, close], bandw.exp = be))
# #         colnames(fdGPH_) <- paste0(colnames(fdGPH_), "_fdGPH_", be)
# #     fdSperio_ <- as.data.frame(fracdiff::fdSperio(tsfeature_dt[, close], bandw.exp = be, beta = 0.9))
# #         colnames(fdSperio_) <- paste0(colnames(fdSperio_), "_fdSperio_", be)
# #         cbind(fdGPH_, fdSperio_)
# #     })

# # vars_ds <- unlist(ds)
# # names(vars_ds) <- gsub("//.", "_", names(vars_ds))
# # vars <- c(vars, vars_ds)

# # # clean names
# # names(vars) <- paste0(names(vars), "_", window)
# # data.table(t(vars))