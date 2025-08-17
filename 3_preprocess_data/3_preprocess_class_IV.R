# [3] TIME SERIES MODEL: QUARKS: computing the value at risk (VaR) and expected shortfall (ES) using historical simulation
# 6: ROLLINGQUARKS IS OK
# https://cran.r-project.org/web/packages/quarks/index.html
# @description: Enables the user to calculate Value at Risk (VaR) and Expected Shortfall (ES) by means of various types of historical simulation
# @description: using RETURNS of the stock to compute value at risk and expected shortfall
# @returns Value at Risk (VaR) and Expected Shortfall (ES) based on EWMA/GARCH model through 
# @returns plain/age/volatility weighted historical simulation/filtered historical simulation method
# encouraged to use filtered historical simulation method as proposed by the research paper: ONLY USED FHS

#======= Main Class =======
# create R6 class
feature_four <- R6::R6Class(
	"feature_four",
    private = list(
        load_dependencies = function(){
            required_packages <- c("tidyverse", "checkmate", "duckdb", "data.table", "mirai", "crew", "parallel", "tictoc", "QuantTools", "bidask", "roll", "TTR", "PerformanceAnalytics",
				"binomialtrend", "vse4ts", "exuber", "GAS", "tvgarch", "quarks")
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
        rolling_function = function(x, window, model, method){
        # @description: the main algorithm for Quarks
        # @params: x is the dataframe, containing date, symbol and ohlcv
        # @params: window: size of the rolling window
        # @params: model: the model to be used: can be c("EWMA", "GARCH")
        # @params: methods: the methods to be used: can be ("plain", "age", "vwhs", "fhs"): FHS need to have nboot
        # @returns: Value at Risk (VaR) and Expected Shortfall (ES), normalised to future return
            template_cols <- c(paste0("var_", model, "_", method, "_", window), paste0("es_", model, "_", method, "_", window))
            empty_row <- data.table::as.data.table(
                setNames(as.list(rep(NA_real_, length(template_cols))), template_cols)
            )

            # compute returns
            if (length(unique(x$symbol)) > 1) return(empty_row)

            # main algorithm, dependent on method
            if (method == "fhs"){
                y <- tryCatch(
                    base::suppressMessages(
                        quarks::rollcast(
                            x = x$returns,
                            model = model,
                            method = method,
                            nout = 1,
                            nwin = window - 1,
                            nboot = 500
                        )
                    ), error = function(e) NULL)
                if (is.null(y)) return(empty_row)
            } else {
                y <- tryCatch(
                    base::suppressMessages(
                        quarks::rollcast(
                            x = x$returns,
                            model = model,
                            method = method,
                            nout = 1,
                            nwin = window - 1
                        )
                    ), error = function(e) NULL)
                if (is.null(y)) return(empty_row)
            }
            

            VaR <- (y$xout - abs(y$VaR))/y$xout # [normalisation] quantifying downside risk to expected future return (VaR as a proportion of forecasted return)
            ES <- (y$xout - abs(y$ES))/y$xout # [normalisation] quantifying downside risk to expected future return (Expected Shortfall as a proportion of forecasted return)

            results <- data.table::as.data.table(
                t(setNames(
                    c(VaR, ES),
                    template_cols
                ))
            )
            return(results[])
        },
        #=========================================== MAIN CODE TO RUN ===============================================
        run_code = function(subset_df, windows_, price_col, rolling_function, model, method){
            # @description: uses the mirai package to integrate asynchronous parallel programming into the script

            subset_df[, returns := get(price_col) / data.table::shift(get(price_col)) - 1][!is.na(returns)] # compute returns, removes first row

            n_rows <- nrow(subset_df)
            indices <- (windows_ + 1):n_rows
            n_cores <- self$num_cores # additional
            chunked_indices <- split(indices, cut(seq_along(indices), n_cores, labels = FALSE)) # additional

            result_dt <- data.table(date = subset_df$date)

            results <- mirai::mirai_map(
                .x = chunked_indices,
                .f = function(index_chunk, dt, windows_, rolling_function, price_col, model, method) {
                    data.table::setDTthreads(2)  # Allow light multithreading inside workers
                    out <- lapply(index_chunk, function(i) {
                        window_data <- dt[(i - windows_ + 1):i]
                        rolling_function(window_data, windows_, model, method)
                })
                data.table::rbindlist(out, fill = TRUE)
                },
                .args = list(
                    dt = subset_df,
                    windows_ = windows_,
                    price_col = price_col,
                    model = model,
                    method = method,
                    rolling_function = rolling_function
                )
            )[]
            res_matrix <- do.call(rbind, results)
            stat_names <- colnames(res_matrix)

            for (j in seq_along(stat_names)) {
                colname <- paste0("quarks_", stat_names[j])
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
            data.table::setDTthreads(threads = 0, throttle = 1)
            setorder(ohlcv, symbol, date)
            
            print("=================== [6] COMPUTING VaR and Expected Shortfall from Quarks Package ===================")
            #=================== OTHER INDICATORS 6: VaR and Expected Shortfall from Quarks Package ===================
            print("[6] Calculating VaR and Expected Shortfall from Quarks Package...")
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
            models <- c("EWMA", "GARCH")
            methods <- c("plain", "age", "vwhs", "fhs")
            # grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
            # ======================================= the code to be runned =======================================
            # focused on fhs because it is what the author encourages
            try_list1 <- as.data.table(self$run_code(symbol_df, windows_, price_col, self$rolling_function, model = "GARCH", method = "fhs"))
            # try_list2 <- as.data.table(self$run_code(symbol_df, windows_, price_col, self$rolling_function, model = "EWMA", method = "fhs"))
            results_dt <- try_list1
            # results_dt <- data.table::rbindlist(list(try_list1,try_list2), use.names = TRUE, fill = TRUE)
            # results_dt <- data.table::rbindlist(try_list, use.names = TRUE, fill = TRUE)
            data.table::setkey(results_dt, symbol, date)
            new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
            ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

            end_time <- tictoc::toc()
            quarks_time <- round(end_time$toc - end_time$tic,3)
            print(paste0("Total Time Taken to Calculating VaR and Expected Shortfall from Quarks Package ", quarks_time, " Seconds"))
            rm(start_time, windows_, price_col, models, methods, try_list1, results_dt, new_cols, end_time, quarks_time)
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
# class(prepare_df)

# ================================================================ END OF CODE, TEST NOW ================================================================
# # set up
# dt = df["aapl"]
# price_col <- "close"
# model <- "GARCH" # models <- c("EWMA", "GARCH"): use GARCH for now
# method <- "age" # method <- c("plain", "age", "vwhs", "fhs"): use fhs for now
# dt[, returns := get(price_col) / data.table::shift(get(price_col)) - 1][!is.na(returns)] # compute returns, removes first row
# windows_ = 252 # rolling window: 252
# n_rows <- nrow(dt)
# indices <- (windows_ + 1):n_rows # start from 253
# result_dt <- data.table(date = dt$date)

# i = indices[1]


# b <- mirai_function(i, dt, windows_, rolling_function, model, method)

# c <- run_code(dt, windows_, "close", rolling_function, model = "GARCH", method = "fhs")

# d <- other_finfeatures_indicators(df)

# tail(d["a"])
# tail(d["aa"])

# #========================================================= CREATING ROLLING FUNCTIONS =========================================================
# rolling_function = function(x, window, model, method){
#     # @description: the main algorithm for Quarks
#     # @params: x is the dataframe, containing date, symbol and ohlcv
#     # @params: window: size of the rolling window
#     # @params: model: the model to be used: can be c("EWMA", "GARCH")
#     # @params: methods: the methods to be used: can be ("plain", "age", "vwhs", "fhs"): FHS need to have nboot
#     # @returns: Value at Risk (VaR) and Expected Shortfall (ES), normalised to future return
#     template_cols <- c(paste0("var_", model, "_", method, "_", window), paste0("es_", model, "_", method, "_", window))
#     empty_row <- data.table::as.data.table(
#         setNames(as.list(rep(NA_real_, length(template_cols))), template_cols)
#     )

#     # compute returns
#     if (length(unique(x$symbol)) > 1) return(empty_row)

#     # main algorithm, dependent on method
#     if (method == "fhs"){
#         y <- tryCatch(
#             base::suppressMessages(
#                 quarks::rollcast(
#                     x = x$returns,
#                     model = model,
#                     method = method,
#                     nout = 1,
#                     nwin = window - 1,
#                     nboot = 500
#                 )
#             ), error = function(e) NULL)
#         if (is.null(y)) return(empty_row)
#     } else {
#         y <- tryCatch(
#             base::suppressMessages(
#                 quarks::rollcast(
#                     x = x$returns,
#                     model = model,
#                     method = method,
#                     nout = 1,
#                     nwin = window - 1
#                 )
#             ), error = function(e) NULL)
#         if (is.null(y)) return(empty_row)
#     }
    

#     VaR <- (y$xout - abs(y$VaR))/y$xout # [normalisation] quantifying downside risk to expected future return (VaR as a proportion of forecasted return)
#     ES <- (y$xout - abs(y$ES))/y$xout # [normalisation] quantifying downside risk to expected future return (Expected Shortfall as a proportion of forecasted return)

#     results <- data.table::as.data.table(
#         t(setNames(
#             c(VaR, ES),
#             template_cols
#         ))
#     )
#     return(results[])
# }
# #========================================================= CREATING MIRAI FUNCTIONS =========================================================
# mirai_function = function(i, dt, windows_, rolling_function, model, method){
#     # @description: helps consolidate everything needed to be placed in the parent function
#     # @params: i is the indice, basically the iteration number. this controls which subset of data to use
#     # @params: dt and windows_ are the data and size of window respectively which is to be used in the main algorithm function, rolling_function
#     # @returns: the results from the function rolling_function
#     data.table::setDTthreads(1)
#     window_data <- dt[(i - windows_ + 1):i]
#     rolling_function(window_data, windows_, model, method)
# }
# #=========================================== MAIN CODE TO RUN ===============================================
# run_code = function(subset_df, windows_, price_col, rolling_function, model, method){
#     # @description: uses the mirai package to integrate asynchronous parallel programming into the script
#     # @params: forcing windows to be 504 and price column to be close

#     subset_df[, returns := get(price_col) / data.table::shift(get(price_col)) - 1][!is.na(returns)] # compute returns, removes first row

#     n_rows <- nrow(subset_df)
#     indices <- (windows_ + 1):n_rows

#     result_dt <- data.table(date = subset_df$date)

#     results <- mirai::mirai_map(
#         .x = indices,
#         .f = mirai_function, # need add a self$ in front
#         .args = list(
#             dt = subset_df,
#             windows_ = windows_,
#             model = model,
#             method = method,
#             rolling_function = rolling_function
#         )
#     )[]
#     res_matrix <- do.call(rbind, results)
#     stat_names <- colnames(res_matrix)

#     for (j in seq_along(stat_names)) {
#         colname <- paste0("quarks_", stat_names[j])
#         result_dt[indices, (colname) := res_matrix[[j]]]
#     }

#     result_dt[, symbol := subset_df$symbol[1]]  # retain symbol
#     new_cols <- colnames(result_dt)[!colnames(result_dt) %in% c("symbol", "date")]
#     subset_df[result_dt, on = .(symbol, date), (new_cols) := mget(paste0("i.", new_cols))]
#     return(subset_df)
# }

#=========================================== CONSOLIDATION CODE ===============================================
# other_finfeatures_indicators = function(df) {
#     # @description: consolidating all functions together to run as 1 function
#     ohlcv <- data.table::copy(df)
#     data.table::setDTthreads(threads = 0, throttle = 1)
#     setorder(ohlcv, symbol, date)
    
#     print("=================== COMPUTING VaR and Expected Shortfall from Quarks Package ===================")
#     Sys.sleep(2)
#     #=================== OTHER INDICATORS 6: VaR and Expected Shortfall from Quarks Package ===================
#     print("[6] Calculating VaR and Expected Shortfall from Quarks Package...")
#     start_time <- tictoc::tic()
    #=========================================== MIRAI function (parallel processing) ===============================================
    # set up mirai for parallel processing
    # if (mirai::daemons()$connections == 0){
    #     mirai::daemons(parallel::detectCores())
    # }

    # mirai::everywhere({
    #     library(data.table)
    #     })
    
    # # Normal Set Up
    # windows_ = 252
    # price_col = "close"
    # models <- c("EWMA", "GARCH")
    # methods <- c("plain", "age", "vwhs", "fhs")
    # grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
    # ======================================= the code to be runned =======================================
    ### Splitting the tickers
#     symbol_list <- split(ohlcv, by = "symbol", keep.by = TRUE)
#     smaller_symbol_list <- symbol_list[1:2] # try on 2 symbol first: REMOVE IF WANT TO RUN THE FULL CODE

#     try_list <- lapply(smaller_symbol_list, function(dt){
#                 run_code(dt, windows_, price_col, rolling_function, model = "GARCH", method = "fhs")})
    
#     # to have more than one
#     try_list2 <- lapply(smaller_symbol_list, function(dt){
#                 run_code(dt, windows_, price_col, rolling_function, model = "EWMA", method = "fhs")})


#     results_dt <- data.table::rbindlist(c(try_list,try_list2), use.names = TRUE, fill = TRUE)
#     # results_dt <- data.table::rbindlist(try_list, use.names = TRUE, fill = TRUE)
#     data.table::setkey(results_dt, symbol, date)
#     new_cols <- colnames(results_dt)[!colnames(results_dt) %in% c("symbol", "date")]
#     print(new_cols)
#     ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))] # might be ohlcv[results_dt, (new_cols) := mget(paste0("i.", new_cols))]

#     end_time <- tictoc::toc()
#     quarks_time <- round(end_time$toc - end_time$tic,3)
#     print(paste0("Total Time Taken to Calculating VaR and Expected Shortfall from Quarks Package ", quarks_time, " Seconds"))
#     # return(res_list)s
#     return(ohlcv) # might be return(ohlcv)
# }

# CJ(sym_idx = seq_along(smaller_symbol_list), grid_idx = seq_len(nrow(grid)))

# if (mirai::daemons()$connections == 0){
#     mirai::daemons(parallel::detectCores())
# }

# mirai::everywhere({
#     library(data.table)
#     })


# models <- c("EWMA", "GARCH")
# methods <- c("plain", "age", "vwhs", "fhs")
# grid <- base::expand.grid(model = models, method = methods, stringAsFactors = FALSE)
# grid$model[1]
# grid$method[1]
