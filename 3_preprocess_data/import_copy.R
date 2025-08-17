#' @description consolidating information related to stocks (that are not OHLCV and features)
#' @note replication from Mr Mislav's import script in findata package: https://github.com/MislavSag/findata/blob/main/R/Import.R
#' @note for those that dont have dividends/earning report, will be given an empty data table
#' @example data$dividends[symbol == "EMA"] will give an empty data table
#' @example other special cases (other than EMA): NMAX & VOYG
#' @return 4 data tables saved in a list: events, fundamentals, prices, dividends
#' @note import.R is dependent on extract_data_fmp_class.R
#' @note extract_data_fmp_class.R IS THE CORE SCRIPT
#' @warning !!! HAS ABSOLUTE PATHS: NEED TO BE CAREFUL!!!
#' @note has an algorithm that keeps trying if parallel computing fails (max 3 attemps)

data_related_to_stock = R6::R6Class(
    classname = "data_related_to_stock",
    private = list(
        load_dependencies = function(){
            #' @description loads the R packages used in this class
            #' @note if the packages is missing in the laptop, install the packages for them
            required_packages <- c("tidyverse", "httr", "data.table", "purrr", "duckdb" ,"parallel", "future", "future.apply", "lubridate", "jsonlite")
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

            #===== [Might need to be refined when there is error] Need to find extract_data_fmp_class =====
            # TEST
            path_name <- '/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/fmp_data_after_feature_engineering_in_graveyard/extract_data_fmp_class_copy.R'
            source(path_name)
            message("extract_data_fmp_class has been loaded")
        }
    ),
    public = list(
        #===== Declaring Some Fields =====
        FMP_API_KEY = NULL,
        #===== Initialising some stuff =====
        initialize = function(FMP_API_KEY = NULL){
            private$load_dependencies()
            self$FMP_API_KEY="u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"
        },
        load_data = function(current_path, database_name, table_name){
            #' @description: load the MARKET data from duckdb (provided by quantconnect where they got the ohlcv equity data from algoseek)
            #' @param current_path: current path to the file
            #' @param database_name: name of the database, basically the name of the db (without the db)
            #' @param table_name: name of the table inside the database, use DBI::dbListTables() to see
            #' @return the daily historical data table for the entire US equity
            #' @note from 3_preprocess_class_draft2.R
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

            return(df)
        },
        get_data = function(symbols = NULL){
            #' @param symbols: the symbols requiring information extractions
            #' @return events, fundamentals, prices, dividends which is what was given in Mr Mislav's imports.R
            instance_1 <- extract_data_FMP$new() # to use functions from other classes
            instance_2 <- self # to use functions within the class
            
            if (is.null(symbols)){
                #==================== get stocks from S&P500 ====================
                securities <- instance_1$get_index_companies("sp500") # all tickers
                symbols <- securities$symbol
                symbols <- symbols[1:200] # TEST: TO BE REMOVED
                symbols <- c("SPY", symbols)
            } else if (length(symbols) == 1 && tolower(symbols) == "spy") {
                #' @note if it is a single stock: spy
                symbols <- "SPY"
            } else {
                symbols <- symbols[1:201] # TEST: TO BE REMOVED: ADDING SPY IN
            }
            
            #==================== profiles ====================
            #' @description profiles of all stocks in the US, including ADRs
            #' @note symbols_us_profiles will take some time to load

            results <- parallel::mclapply(symbols, function(symbol){
                #' @note ensures that even if the cores fail to run, it will rerun again until it processes the data properly
                max_retries <- 5 # max 3 tries
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success){
                    tryCatch({
                        instance <- extract_data_FMP$new()
                        result <- instance$get_company_profiles(symbol)
                        success <- TRUE
                    }, error = function(e){
                        message(sprintf("Error on attempt %d for %s: %s", attempt, symbol, e$message))
                        attempt <<- attempt + 1
                        Sys.sleep(0.5 * attempt)
                    })
                }

                if (!success) {
                    message(sprintf("Failed to get profile for %s after %d attempts", symbol, max_retries))
                    return(NULL)
                }

                return(result)
            }, mc.cores = 16)

            cleaned_df <- rbindlist(results, fill = TRUE, use.names = TRUE) %>% as.data.table()
            us_profiles <- cleaned_df[country == "US" & is_etf == FALSE & is_fund == FALSE] # remove all that are type funds and etfs
            setnames(us_profiles, tolower(colnames(us_profiles)))
            message("DONE: US PROFILES")

            #==================== market capitalisation ====================
            #' @description market capitalisation of all stocks in the US: using symbols_us
            #' @note there is only 5 years worth of data here
            #' @note robustness executed by rerunning the code if necessary (3 times), in case of error
            
            symbols_us <- us_profiles$symbol # only those in US

            market_capitalisation_list <- parallel::mclapply(symbols_us, function(symbol) {
                #' @note ensures that even if the cores fail to run, it will rerun again until it processes the data properly
                max_retries <- 5 # max 3 tries
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success) {
                    tryCatch({
                        result <- instance_1$get_historical_market_capitalization(symbol)
                        success <- TRUE}, 
                    error = function(e) {
                        message(sprintf("Error on attempt %d for %s: %s", attempt, symbol, e$message))
                        attempt <<- attempt + 1
                        Sys.sleep(0.5 * attempt)  # Linear backoff
                        })
                }

                if (!success) {
                    message(sprintf("Failed to get data for %s after %d attempts", symbol, max_retries))
                    return(NULL)
                }
                return(result)}, mc.cores = 16)
            
            market_capitalisation_dt <- rbindlist(market_capitalisation_list, use.names = TRUE, fill = TRUE)
            setkey(market_capitalisation_dt, symbol, market_capitalization_date)
            setorder(market_capitalisation_dt, symbol, -market_capitalization_date)

            message("DONE: US MARKET CAPITALISATION")

            #========================= [EVENT] earning announcement =========================
            #' @description earnings announcement of all stocks in the US: using symbols_us
            #' @note robustness executed by rerunning the code if necessary (3 times), in case of error
            #' @return earnings_announcement_dt
            
            earnings_announcement_list <- parallel::mclapply(symbols_us, function(symbol) {
                max_retries <- 5 # max 3 tries
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success) {
                    tryCatch({
                        result <- instance_1$get_earnings_announcement(symbol)
                        success <- TRUE}, 
                    error = function(e) {
                        message(sprintf("Error on attempt %d for %s: %s", attempt, symbol, e$message))
                        attempt <<- attempt + 1
                        Sys.sleep(0.5 * attempt)  # Linear backoff
                        })
                }

                if (!success) {
                    message(sprintf("Failed to get data for %s after %d attempts", symbol, max_retries))
                    return(NULL)
                }
                return(result)}, mc.cores = 16)
            
            # NMAX, EMA, VOYG has errors: need implement more robust algorithms in my class object
            earnings_announcement_dt <- rbindlist(earnings_announcement_list, use.names = TRUE, fill = TRUE)
            earnings_announcement_dt <- earnings_announcement_dt[earnings_announcement_date < Sys.Date()]
            earnings_announcement_dt <- unique(earnings_announcement_dt, by = c("symbol", "earnings_announcement_date")) # might have issue
            earnings_announcement_dt <- earnings_announcement_dt[symbol %in% symbols_us]
            setkey(earnings_announcement_dt, symbol, earnings_announcement_date)
            setorder(earnings_announcement_dt, symbol, -earnings_announcement_date)

            message("DONE: US EARNING ANNOUNCEMENT")
            #========================= [FUNDAMENTALS] FUNDAMENTALS =========================
            #' @description fundamentals: items from balance sheet, income statement, cashflow statement, finanical growth, financial metrics, financial ratios
            #' @note robustness executed by rerunning the code if necessary (3 times), in case of error
            #' @return fundamentals_dt
            
            fundamentals_list <- parallel::mclapply(symbols_us, function(symbol) {
                max_retries <- 5 # max 3 tries
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success) {
                    tryCatch({
                        result <- instance_1$get_fundamental_data(symbol)
                        success <- TRUE}, 
                    error = function(e) {
                        message(sprintf("Error on attempt %d for %s: %s", attempt, symbol, e$message))
                        attempt <<- attempt + 1
                        Sys.sleep(0.5 * attempt)  # Linear backoff
                        })
                }

                if (!success) {
                    message(sprintf("Failed to get data for %s after %d attempts", symbol, max_retries))
                    return(NULL)
                }
                return(result)}, mc.cores = 16)
            
            fundamentals_dt <- rbindlist(fundamentals_list, use.names = TRUE, fill = TRUE)
            message("DONE: FUNDAMENTALS")
            #========================= [PRICES] MARKET DATA =========================
            #' @description market data from quantconnect
            #' @note adds industry and sector information to the stock
            #' @note adds market capitalization as well (INNER JOIN)
            #' @note adds weighted shares (normal & diluted)
            #' @return prices
            
            # get data from our quantconnect data
            prices <- instance_2$load_data('/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate', "stocks_daily", "stocks_daily")
            prices[, returns := adj_close / data.table::shift(adj_close) - 1, by = symbol]

            # add profiles data
            us_profiles[, symbol := tolower(symbol)]
            prices = us_profiles[, .(symbol, industry, sector)][prices, on = "symbol"] # OK

            market_cap = copy(market_capitalisation_dt)
            setnames(market_cap, "market_capitalization_date", "date")
            market_cap[, symbol := tolower(symbol)]

            # add market cap data
            #' @note inner join (between OHLCV and Market Capitalisation DATES), if want to do right join (on prices), remove nomatch = 0L
            #' @note justifiable because it is likely the market capitalisation changes daily
            prices = market_cap[prices, on = c("symbol", "date"), nomatch = 0L]
            
            # if (!any(tolower(symbols) == "spy")){
            #     #' @note check if any of the symbol is the market data
            #     fundamentals_dt[, symbol := tolower(symbol)]
                
            #     # add number of shares
            #     fundamental_merge <- fundamentals_dt[, .(symbol,
            #         date = report_date,
            #         income_statement_weighted_average_shs_out_quarter,
            #         income_statement_weighted_average_shs_out_dil_quarter)]

            #     #' @note since share-related data is typically updated quarterly, a forward rolling join is used to align the most recent available fundamentals with higher-frequency price data.
            #     prices = fundamental_merge[prices, on = c("symbol", "date"), roll = Inf]
            # }
            
            fundamentals_dt[, symbol := tolower(symbol)]
            prices[, symbol := tolower(symbol)]

            # Separate non-SPY and SPY prices
            non_spy_prices <- prices[symbol != "spy"]
            spy_prices <- prices[symbol == "spy"]

            # add number of shares
            fundamental_merge <- fundamentals_dt[, .(symbol,
                date = report_date,
                income_statement_weighted_average_shs_out_quarter,
                income_statement_weighted_average_shs_out_dil_quarter)]

            #' @note since share-related data is typically updated quarterly, a forward rolling join is used to align the most recent available fundamentals with higher-frequency price data.
            # prices = fundamental_merge[prices, on = c("symbol", "date"), roll = Inf]
            non_spy_prices <- fundamental_merge[non_spy_prices, on = c("symbol", "date"), roll = Inf]
            prices <- rbind(non_spy_prices, spy_prices, use.names = TRUE, fill = TRUE)

            setorder(prices, symbol, -date)
            
            message("DONE: PRICES (MARKET DATA)")
            #========================= [DIVIDENDS] DIVIDENDS =========================
            #' @description dividends
            #' @note robustness executed by rerunning the code if necessary (3 times), in case of error
            #' @return dividends_dt
            
            dividends_list <- parallel::mclapply(symbols_us, function(symbol) {
                max_retries <- 5
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success) {
                    tryCatch({
                        result <- instance_1$get_dividends(symbol)
                        success <- TRUE}, 
                    error = function(e) {
                        message(sprintf("Error on attempt %d for %s: %s", attempt, symbol, e$message))
                        attempt <<- attempt + 1
                        Sys.sleep(0.5 * attempt)  # Linear backoff
                        })
                }

                if (!success) {
                    message(sprintf("Failed to get data for %s after %d attempts", symbol, max_retries))
                    return(NULL)
                }
                return(result)}, mc.cores = 16)

            dividends_dt <- rbindlist(dividends_list, use.names = TRUE, fill = TRUE)
            setorder(dividends_dt, symbol, -ex_dividend_date)
            
            message("DONE: DIVIDENDS")
            #========================= CONSOLIDATION =========================
            #' @description ensures that the symbol columns in all data tables are in UPPER
            prices[, symbol := toupper(symbol)]
            fundamentals_dt[, symbol := toupper(symbol)]
            earnings_announcement_dt[, symbol := toupper(symbol)]
            dividends_dt[, symbol := toupper(symbol)]

            return(list(
                events = earnings_announcement_dt,
                fundamentals = fundamentals_dt,
                prices = prices,
                dividends = dividends_dt
            ))
        }
    )
)

#======= TEST =========
# start <- data_related_to_stock$new()
# data <- start$get_data()

# data$dividends
# view(data$events)
# view(data$fundamentals)
# view(data$prices)
# view(data$dividends)

# #AVGO , MHK

# view(data$prices[symbol == "AVGO"])
# view(data$prices[symbol == "MHK"])
