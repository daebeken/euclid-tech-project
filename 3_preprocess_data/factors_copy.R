#' @description forming the get_factors function first
#' @description the most important script for processing: [1: company related] price factors AND fundamental factors [2: macro related] macro factors
#' @note replicated from Mr Mislav's https://github.com/MislavSag/findata/blob/main/R/Factors.R#L55
#' @note factors.R is dependent on import.R
#' @note import.R is dependent on extract_data_fmp_class.R
#' @note extract_data_fmp_class.R IS THE CORE SCRIPT
#' @return 3 data tables: price factors, fundamental factors and macros
#' @warning !!! HAS ABSOLUTE PATHS: NEED TO BE CAREFUL!!!

get_factors = R6::R6Class(
    classname = "get_factors",
    private = list(
        load_dependencies = function(){
            #' @description loads the R packages used in this class
            #' @note if the packages is missing in the laptop, install the packages for them
            required_packages <- c("tidyverse", "httr", "data.table", "purrr", "duckdb" ,"parallel", "future", "future.apply", "lubridate", "jsonlite", "fredr")
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
            extract_data_fmp_class_path <- '/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/fmp_data_after_feature_engineering_in_graveyard/extract_data_fmp_class_copy.R'
            import_data_class_path <- '/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/fmp_data_after_feature_engineering_in_graveyard/import_copy.R'
            source(extract_data_fmp_class_path)
            source(import_data_class_path)
            message("extract_data_fmp_class and import_data_class has been loaded")
            plan(multisession, workers = 16)
        }
    ),
    public = list(
        #===== Declaring Some Fields =====
        FMP_API_KEY = NULL,
        FREDR_API_KEY = NULL,
        trading_year = NULL,
        trading_halfyear = NULL,
        trading_quarter = NULL,
        #===== Initialising some stuff =====
        initialize = function(FMP_API_KEY = NULL, FREDR_API_KEY = NULL, trading_year = NULL,
            trading_halfyear = NULL, trading_quarter = NULL){
            private$load_dependencies()
            self$FMP_API_KEY="u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH" # Nico's API Key (Subscription)
            self$FREDR_API_KEY = "fb7c1b60dacc0688c8f88eca129146f9" # Arthur's API Key (Free)
            fredr::fredr_set_key(self$FREDR_API_KEY)
            self$trading_year <- 256
            self$trading_halfyear <- self$trading_year/2
            self$trading_quarter <- self$trading_year/4
        },
        #===== HELPER FUNCTIONS =====
        future_return = function(x,n){
            #' @param x: the price (in data table)
            #' @param n: the number of days (of forward lag)
            #' @return the returns based on the number of days of lag
            #' @example if n=3, compare day 4's price with day 1's price
            (data.table::shift(x, n, type = "lead") - x) / x
        },
        
        weighted_mean = function(x, w, ..., na.rm=FALSE){
            #' @description helper function to compute the weighted mean
            #' @param x: an object containing the values whose weighted mean is to be computed
            #' @param w: a numerical vector of weights the same length as x giving the weights to use for elements of x
            #' @return weighted mean based on stats package
            
            if(na.rm){
                keep = !is.na(x)&!is.na(w)
                w = w[keep]
                x = x[keep]
            }
            weighted.mean(x, w, ..., na.rm=FALSE)
        },
        #=========================== MAIN ALGORITHM: GET_FACTOR_DATA ===========================
        #=========================== MAIN ALGORITHM: GET_FACTOR_DATA ===========================
        #=========================== MAIN ALGORITHM: GET_FACTOR_DATA ===========================
        get_factor_data = function(symbols = NULL){
            try1 <- self
            fmp <- extract_data_FMP$new() # from extract_data_fmp_class.R, all the useful functions to extract data
            import_data <- data_related_to_stock$new() # from imports.R, data_related_to_stock class

            #=========================== Loading Information ===========================
            # get US Companies
            if (is.null(symbols)){
                #==================== get stocks from S&P500 ====================
                securities <- fmp$get_index_companies("sp500") # all tickers
                symbols <- securities$symbol
                symbols <- symbols[1:200] # TEST: TO BE REMOVED
                symbols <- c("SPY", symbols)
            } else if (length(symbols) == 1 && tolower(symbols) == "spy") {
                #' @note if it is a single stock: spy
                symbols <- "SPY"
            } else {
                symbols <- symbols[1:201] # TEST: TO BE REMOVED: ADDING SPY IN
            }

            results <- parallel::mclapply(symbols, function(symbol){
                #' @note ensures that even if the cores fail to run, it will rerun again until it processes the data properly
                max_retries <- 3 # max 3 tries
                attempt <- 1
                result <- NULL
                success <- FALSE

                while (attempt <= max_retries && !success){
                    tryCatch({
                        # instance <- extract_data_FMP$new()
                        result <- fmp$get_company_profiles(symbol)
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
            symbols_us <- us_profiles$symbol # only those in US
            symbols_us1 <- c("SPY", symbols_us)


            # get sp500 symbols
            # test this out: not sure correct or not in terms of the s&p500: current or historical?
            # scrape_information <- httr::GET(paste0("https://financialmodelingprep.com/api/v3/historical/sp500_constituent?apikey=", fmp_apikey)) # historical
            sp500_companies <- fmp$get_index_companies(index = "sp500")
            sp500_symbols <- sp500_companies$symbol

            message("[1] Data Loaded")
            #=========================== Extracting Data from Symbols ===========================
            fmp_data <- import_data$get_data(symbols = symbols_us1) # using previous code
            # separate into the 4 types: events (earning announcement), fundamentals, prices, dividends
            events <- fmp_data$events
            fmp_data$events <- NULL

            fundamentals <- fmp_data$fundamentals
            fmp_data$fundamentals <- NULL

            prices <- fmp_data$prices
            fmp_data$prices <- NULL

            dividends = fmp_data$dividends
            fmp_data$dividends <- NULL
            rm(fmp_data) # redundant
            message("[2] Relevant Available Information Extracted for All Symbols")

            #=========================== Cleaning Prices ===========================
            prices <- unique(prices, by = c("symbol", "date"))
            setorder(prices, symbol, date)

            # removing observations with extreme prices
            prices <- prices[returns < 0.9 & returns > -0.9]
            prices[, high_return := high / shift(high) - 1, by = symbol]
            prices <- prices[high_return < 0.9 & high_return > -0.9]
            prices[, low_return := low / shift(low) - 1, by = symbol]
            prices <- prices[low_return < 0.9 & low_return > -0.9]
            prices[, `:=`(low_return = NULL, high_return = NULL)]

            # remove observation with less than 2 years of data
            prices_n <- prices[, .N, by = symbol]
            prices_n <- prices_n[N > (self$trading_year * 2)]  # remove prices with only 700 or less observations
            prices <- prices[symbol %in% prices_n[, symbol]]
            setorder(prices, symbol, date)

            message("[3] Cleaning Prices Done")

            # create a separate data table for spy
            spy <- prices[symbol == "SPY"]
            #=========================== EVENTS ===========================
            #=========================== EVENTS ===========================
            #=========================== EVENTS ===========================
            # create predictors from earnings announcements

            # i have to set the order first because the latest earning announcement date came first
            setorder(events, symbol, earnings_announcement_date)
            events[, `:=`(
                #' @description computes the number of EPS beats occurred in the past n quarters
                #' @note nincr: number of increases
                #' @example nincr: counts the number of EPS beats occuring in the last 4 quarters
                nincr_half = frollsum(earnings_announcement_eps > earnings_announcement_eps_estimated, 2, na.rm = TRUE),
                nincr = frollsum(earnings_announcement_eps > earnings_announcement_eps_estimated, 4, na.rm = TRUE),
                nincr_2y = frollsum(earnings_announcement_eps > earnings_announcement_eps_estimated, 8, na.rm = TRUE),
                nincr_3y = frollsum(earnings_announcement_eps > earnings_announcement_eps_estimated, 12, na.rm = TRUE),
                eps_diff = (earnings_announcement_eps - earnings_announcement_eps_estimated + 0.00001) / (earnings_announcement_eps_estimated + 0.00001)
            ), by = symbol]

            message("[4] Predictors Earning Announcement Done")

            #=========================== PRICES ===========================
            #=========================== PRICES ===========================
            #=========================== PRICES ===========================
            setorder(prices, "symbol", "date") # order, to be sure

            ##=========================== Momentum ===========================##
            prices[, mom1w := close / shift(close, 5) - 1, by = symbol]
            prices[, mom1w_lag := shift(close, 10) / shift(close, 5) - 1, by = symbol]

            prices[, mom1m := close / shift(close, 22) - 1, by = symbol]
            prices[, mom1m_lag := shift(close, 22) / shift(close, 44) - 1, by = symbol]

            prices[, mom6m := close / shift(close, 22 * 6) - 1, by = symbol]
            prices[, mom6m_lag := shift(close, 22) / shift(close, 22 * 7) - 1, by = symbol]

            prices[, mom12m := close / shift(close, 22 * 12) - 1, by = symbol]
            prices[, mom12m_lag := shift(close, 22) / shift(close, 22 * 13) - 1, by = symbol]

            prices[, mom36m := close / shift(close, 22 * 36) - 1, by = symbol]
            prices[, mom36m_lag := shift(close, 22) / shift(close, 22 * 37) - 1, by = symbol]

            prices[, chmom := mom6m / shift(mom6m, 22) - 1, by = symbol] # measures change in the 6-month momentum over the past month
            prices[, chmom_lag := shift(mom6m, 22) / shift(mom6m, 44) - 1, by = symbol]
            message("[5] OHLCV Predictors: Momentum Done")

            ##=========================== Maximum and Minimum Return ===========================##
            prices[, maxret_3y := roll::roll_max(returns, self$trading_year * 3), by = symbol]
            prices[, maxret_2y := roll::roll_max(returns, self$trading_year * 2), by = symbol]
            prices[, maxret := roll::roll_max(returns, self$trading_year), by = symbol]
            prices[, maxret_half := roll::roll_max(returns, self$trading_halfyear), by = symbol]
            prices[, maxret_quarter := roll::roll_max(returns, self$trading_quarter), by = symbol]

            prices[, minret_3y := roll::roll_min(returns, self$trading_year * 3), by = symbol]
            prices[, minret_2y := roll::roll_min(returns, self$trading_year * 2), by = symbol]
            prices[, minret := roll::roll_min(returns, self$trading_year), by = symbol]
            prices[, minret_half := roll::roll_min(returns, self$trading_halfyear), by = symbol]
            prices[, minret_quarter := roll::roll_min(returns, self$trading_quarter), by = symbol]
            message("[6] OHLCV Predictors: Maximum and Minimum Return Done")

            ##=========================== Sector and Industry Momentum ===========================##
            #' @description sector momentum
            #' @description returns returns by sector, including those symbols that does not have a sector
            #' @details cumulative return over the 22 day window
            #' @return sector, date, sec_returns
            sector_returns <- prices[, .(sec_returns = try1$weighted_mean(returns, market_capitalization, na.rm = TRUE)), by = .(sector, date)]
            sector_returns[, secmom := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
            sector_returns[, secmom_lag := shift(secmom, 22), by = sector]
            sector_returns[, secmom6m := frollapply(sec_returns, 22, function(x) prod(1 + x) - 1), by = sector]
            sector_returns[, secmom6m_lag := shift(secmom6m, 22), by = sector]
            sector_returns[, secmomyear := frollapply(sec_returns, 22 * 12, function(x) prod(1 + x) - 1), by = sector]
            sector_returns[, secmomyear_lag := shift(secmomyear, 22), by = sector]
            sector_returns[, secmom2year := frollapply(sec_returns, 22 * 24, function(x) prod(1 + x) - 1), by = sector]
            sector_returns[, secmom2year_lag := shift(secmom2year, 22), by = sector]
            sector_returns[, secmom3year := frollapply(sec_returns, 22 * 36, function(x) prod(1 + x) - 1), by = sector]
            sector_returns[, secmom3year_lag := shift(secmom3year, 22), by = sector]

            #' @description industry momentum
            #' @description returns returns by sector, including those symbols that does not have a sector
            #' @details cumulative return over the 22 day window
            #' @return sector, date, ind_returns
            ind_returns = prices[, .(ind_returns = try1$weighted_mean(returns, market_capitalization, na.rm = TRUE)), by = .(industry, date)]
            ind_returns[, indmom := frollapply(ind_returns, 22, function(x) prod(1 + x) - 1), by = industry]
            ind_returns[, indmom_lag := shift(indmom, 22), by = industry]
            ind_returns[, indmom6m := frollapply(ind_returns, 22, function(x) prod(1 + x) - 1), by = industry]
            ind_returns[, indmom6m_lag := shift(indmom6m, 22), by = industry]
            ind_returns[, indmomyear := frollapply(ind_returns, 22 * 12, function(x) prod(1 + x) - 1), by = industry]
            ind_returns[, indmomyear_lag := shift(indmomyear, 22), by = industry]
            ind_returns[, indmom2year := frollapply(ind_returns, 22 * 24, function(x) prod(1 + x) - 1), by = industry]
            ind_returns[, indmom2year_lag := shift(indmom2year, 22), by = industry]
            ind_returns[, indmom3year := frollapply(ind_returns, 22 * 36, function(x) prod(1 + x) - 1), by = industry]
            ind_returns[, indmom3year_lag := shift(indmom3year, 22), by = industry]


            # merge sector and industry predictors to prices
            prices <- sector_returns[prices, on = c("sector", "date")]
            prices <- ind_returns[prices, on = c("industry", "date")]

            message("[7] OHLCV Predictors: Sector and Industry Momentum Done")
            ##=========================== Volatility ===========================##
            #' @description dollar volume: a liquidity measure which represents the total traded value of a stock
            prices[, dvolume := volume * close]
            prices[, `:=`(
                dolvol = frollsum(dvolume, 22),
                dolvol_halfyear = frollsum(dvolume, self$trading_halfyear),
                dolvol_year = frollsum(dvolume, self$trading_year),
                dolvol_2year = frollsum(dvolume, self$trading_year * 2),
                dolvol_3year = frollsum(dvolume, self$trading_year * 3)
            ), by = symbol]

            prices[, `:=`(
                dolvol_lag = shift(dolvol, 22),
                dolvol_halfyear_lag = frollsum(dolvol_halfyear, 22),
                dolvol_year_lag = frollsum(dolvol_year, 22)
            ), by = symbol]

            message("[8] OHLCV Predictors: Volatility Done")
            ##=========================== Illiquidity ===========================##
            # illiquidity
            prices[, `:=`(
                illiquidity = abs(returns) / volume, #  Illiquidity (Amihud's illiquidity)
                illiquidity_month = frollsum(abs(returns), 22) / frollsum(volume, 22),
                illiquidity_half_year = frollsum(abs(returns), self$trading_halfyear) /frollsum(volume, self$trading_halfyear),
                illiquidity_year = frollsum(abs(returns), self$trading_year) / frollsum(volume, self$trading_year),
                illiquidity_2year = frollsum(abs(returns), self$trading_year * 2) / frollsum(volume, self$trading_year * 2),
                illiquidity_3year = frollsum(abs(returns), self$trading_year * 3) / frollsum(volume, self$trading_year * 3)
            ), by = symbol]
            message("[9] OHLCV Predictors: Illiquidity Done")
            ##=========================== Shares Turnover ===========================##
            # Share turnover
            prices[, `:=`(
                share_turnover = volume / income_statement_weighted_average_shs_out_quarter,
                turn = frollsum(volume, 22) / income_statement_weighted_average_shs_out_quarter,
                turn_half_year = frollsum(volume, self$trading_halfyear) / income_statement_weighted_average_shs_out_quarter,
                turn_year = frollsum(volume, self$trading_year) / income_statement_weighted_average_shs_out_quarter,
                turn_2year = frollsum(volume, self$trading_year * 2) / income_statement_weighted_average_shs_out_quarter,
                turn_3year = frollsum(volume, self$trading_year * 2) / income_statement_weighted_average_shs_out_quarter
            ), by = symbol]

            # Standrd Deviation of Share Turnover
            prices[, `:=`(
                std_turn = roll::roll_sd(share_turnover, 22),
                std_turn_6m = roll::roll_sd(share_turnover, self$trading_halfyear),
                std_turn_1y = roll::roll_sd(share_turnover, self$trading_year)
            ), by = symbol]
            message("[10] OHLCV Predictors: Shares Turnover Done")
            ##=========================== Return Volatility ===========================##
            # return volatility
            prices[, `:=`(
                retvol = roll::roll_sd(returns, width = 22),
                retvol3m = roll::roll_sd(returns, width = 22 * 3),
                retvol6m = roll::roll_sd(returns, width = 22 * 6),
                retvol1y = roll::roll_sd(returns, width = 22 * 12),
                retvol2y = roll::roll_sd(returns, width = 22 * 12 * 2),
                retvol3y = roll::roll_sd(returns, width = 22 * 12 * 3)
            ), by= symbol]

            prices[, `:=`(
                retvol_lag = shift(retvol, 22),
                retvol3m_lag = shift(retvol3m, 22),
                retvol6m_lag = shift(retvol6m, 22),
                retvol1y_lag = shift(retvol1y, 22)
            ), by = symbol]
            message("[11] OHLCV Predictors: Return Volatility Done")
            ##=========================== Idiosyncratic Volatility ===========================##
            # idiosyncratic volatility
            weekly_market_returns <- prices[, .(market_returns = mean(returns, na.rm = TRUE)), by = date]
            weekly_market_returns[, market_returns := frollapply(market_returns, 22, function(x) prod(1+x)-1)]
            prices <- weekly_market_returns[prices, on = "date"]
            id_vol <- na.omit(prices, cols = c("market_returns", "mom1w"))
            #' @description perform a rolling linear regression mom1w ~ market returns
            #' @param roll_lm(x,y,n): x, y are numeric vectors, n is the window size
            #' @example QuantTools::roll_lm(market_returns, mom1w, trading_year * 3)
            #' @example mom1w = market returns, with trading_year * 3 as window size
            #' @return [,3][[1]] extracts the residuals from the regression: output is r out of: alpha, beta, r, r squared
            id_vol <- id_vol[, .(date, e = QuantTools::roll_lm(market_returns, mom1w, self$trading_year * 3)[, 3][[1]]),
                                by = symbol]
            id_vol[, `:=`(
                idvol = roll::roll_sd(e, 22),
                idvol3m = roll::roll_sd(e, 22 * 3),
                idvol6m = roll::roll_sd(e, 22 * 6),
                idvol1y = roll::roll_sd(e, self$trading_year),
                idvol2y = roll::roll_sd(e, self$trading_year * 2),
                idvol3y = roll::roll_sd(e, self$trading_year * 3)
            )]
            prices <- id_vol[prices, on = c("symbol", "date")]

            message("[12] OHLCV Predictors: Idiosyncratic Volatility Done")

            ##=========================== Volume of Growth/Decline In Stocks ===========================##
            # volume of growth / decline stocks
            #' @description 52*5 is used because it represents a year's worth of trading data
            vol_52 <- prices[, .(symbol, date, high, low, close, volume)]
            vol_52[, return_52w := close / shift(close, n = 52 * 5) - 1, by = "symbol"]
            vol_52[return_52w > 0, return_52w_dummy := "up", by = "symbol"]
            vol_52[return_52w <= 0, return_52w_dummy := "down", by = "symbol"]
            vol_52 <- vol_52[!is.na(return_52w_dummy)]
            vol_52_indicators <- vol_52[, .(volume = sum(volume, na.rm = TRUE)), by = c("return_52w_dummy", "date")]
            setorderv(vol_52_indicators, c("return_52w_dummy", "date"))
            vol_52_indicators <- dcast(vol_52_indicators, date ~ return_52w_dummy, value.var = "volume")
            vol_52_indicators[, volume_down_up_ratio := down / up]
            vol_52_indicators <- vol_52_indicators[, .(date, volume_down_up_ratio)]

            message("[13] OHLCV Predictors: Volume of Growth/Decline In Stocks Done")

            ##=========================== Welch Goyal ===========================##
            #' @description constructs the Welch-Goyal “Net Equity Issuance” predictor
            #' @description based on the idea that changes in market capitalization not explained by returns are due to net equity issuance 
            #' (e.g., share buybacks or new equity offerings)
            #' @return ntis: net equity issuance

            welch_goyal <- prices[, .(symbol, date, market_capitalization, returns)]
            welch_goyal <- welch_goyal[, .(mcap = sum(market_capitalization, na.rm = TRUE), vwretx = try1$weighted_mean(returns, market_capitalization, na.rm = TRUE)), by = date]

            setorder(welch_goyal, date)

            welch_goyal[, ntis := mcap - (shift(mcap, 22) * (1 + frollapply(vwretx, 22, function(x) prod(1+x)-1)))]
            welch_goyal[, ntis3m := mcap - (shift(mcap, 22 * 3) * (1 + frollapply(vwretx, 22 * 3, function(x) prod(1+x)-1)))]
            welch_goyal[, ntis_halfyear := mcap - (shift(mcap, self$trading_halfyear) * (1 + frollapply(vwretx, self$trading_halfyear, function(x) prod(1+x)-1)))]
            welch_goyal[, ntis_year := mcap - (shift(mcap, self$trading_year) * (1 + frollapply(vwretx, self$trading_year, function(x) prod(1+x)-1)))]
            welch_goyal[, ntis_2year := mcap - (shift(mcap, self$trading_year * 2) * (1 + frollapply(vwretx, self$trading_year*2, function(x) prod(1+x)-1)))]
            welch_goyal[, ntis_3year := mcap - (shift(mcap, self$trading_year * 3) * (1 + frollapply(vwretx, self$trading_year*3, function(x) prod(1+x)-1)))]

            message("[14] OHLCV Predictors: Welch Goyal Done")

            #=========================== DIVIDENDS ===========================#
            #=========================== DIVIDENDS ===========================#
            #=========================== DIVIDENDS ===========================#
            dividends <- na.omit(dividends, cols = c("adj_dividend_amount", "ex_dividend_date"))
            dividends = dividends[symbol %in% sp500_symbols]

            unique(dividends$symbol)

            dividends_sp500 <- dividends[, .(div = sum(adj_dividend_amount , na.rm = TRUE)), by = ex_dividend_date] # group dividends by ex dividend date
            setorder(dividends_sp500, ex_dividend_date)

            # dividends_sp500[, trading_year := lubridate::year(ex_dividend_date)]
            dividends_sp500[, div_year := frollsum(div, self$trading_year, na.rm = TRUE)]
            dividends_sp500[, date := ex_dividend_date]
            dividends_sp500 <- dividends_sp500[spy, on = "date"]

            dividends_sp500[, div_year := nafill(div_year, "locf")]
            dividends_sp500[, dp := div_year / close]
            dividends_sp500[, dy := div_year / shift(close)]
            dividends_sp500 <- na.omit(dividends_sp500[, .(date, dp, dy)])

            welch_goyal <- dividends_sp500[welch_goyal, on = "date"]

            # # reordering
            # desired_order <- c("symbol", "date", "industry", "sector", "income_statement_weighted_average_shs_out_quarter", "income_statement_weighted_average_shs_out_dil_quarter", "open", "high", "low", "close", "volume", "adj_close", "returns")
            # remaining_cols <- setdiff(names(prices), desired_order)
            # setcolorder(prices, c(desired_order, remaining_cols))
            # setorder(prices, symbol, -date)

            #=========================== FUNDAMENTALS ===========================#
            #=========================== FUNDAMENTALS ===========================#
            #=========================== FUNDAMENTALS ===========================#
            #' @description some basic fundamental ratios of the company
            fundamentals[, `:=`(
                total_assets_market_cap_ratio = balance_sheet_statement_total_assets_quarter / financial_metrics_quarterly_market_cap,
                asset_growth_rate = financial_growth_quarterly_asset_growth,
                book_to_market_ratio = ifelse(financial_ratios_quarterly_price_to_book_ratio == 0, NA, 1 / financial_ratios_quarterly_price_to_book_ratio),
                cash_total_asset_ratio = balance_sheet_statement_cash_and_cash_equivalents_quarter / balance_sheet_statement_total_assets_quarter,
                shares_outstanding_growth_rate = financial_growth_quarterly_weighted_average_shares_growth,
                current_ratio = financial_metrics_quarterly_current_ratio,
                dividend_yield_ratio = financial_ratios_quarterly_dividend_yield,
                earnings_to_price_ratio = ifelse(financial_ratios_quarterly_price_to_earnings_ratio == 0, NA, 1 / financial_ratios_quarterly_price_to_earnings_ratio),
                long_term_debt_growth_rate = (balance_sheet_statement_long_term_debt_quarter / shift(balance_sheet_statement_long_term_debt_quarter)) - 1,
                market_cap = financial_metrics_quarterly_market_cap,
                current_ratio_change_rate = (financial_metrics_quarterly_current_ratio / shift(financial_metrics_quarterly_current_ratio)) - 1,
                depreciation_change_rate = (income_statement_depreciation_and_amortization_quarter / shift(income_statement_depreciation_and_amortization_quarter)) - 1,
                rnd_growth_rate = (income_statement_research_and_development_expenses_quarter / shift(income_statement_research_and_development_expenses_quarter)) - 1,
                rnd_market_cap_ratio = income_statement_research_and_development_expenses_quarter / financial_metrics_quarterly_market_cap,
                rnd_sales_ratio = income_statement_research_and_development_expenses_quarter / income_statement_revenue_quarter,
                sales_to_cash_ratio = income_statement_revenue_quarter / balance_sheet_statement_cash_and_cash_equivalents_quarter,
                sales_to_inventory_ratio = income_statement_revenue_quarter / balance_sheet_statement_inventory_quarter,
                sales_to_receivables_ratio = income_statement_revenue_quarter / financial_metrics_quarterly_average_receivables,
                sales_growth_rate = financial_growth_quarterly_revenue_growth,
                sales_to_price_ratio = ifelse(financial_ratios_quarterly_price_to_sales_ratio == 0, NA, 1 / financial_ratios_quarterly_price_to_sales_ratio)
            ), by = symbol]

            new_cols <- c(
                "total_assets_market_cap_ratio", "asset_growth_rate", "book_to_market_ratio", 
                "cash_total_asset_ratio", "shares_outstanding_growth_rate", "current_ratio", 
                "dividend_yield_ratio", "earnings_to_price_ratio", "long_term_debt_growth_rate", 
                "market_cap", "current_ratio_change_rate", "depreciation_change_rate", 
                "rnd_growth_rate", "rnd_market_cap_ratio", "rnd_sales_ratio", 
                "sales_to_cash_ratio", "sales_to_inventory_ratio", "sales_to_receivables_ratio", 
                "sales_growth_rate", "sales_to_price_ratio"
            )

            fundamentals[, (new_cols) := lapply(.SD, function(x) ifelse(is.nan(x), 0, x)), .SDcols = new_cols]
            message("[15] Fundamental Done")

            #=========================== MACRO ===========================#
            #=========================== MACRO ===========================#
            #=========================== MACRO ===========================#
            get_fred <- function(id = "VIXCLS", name = "vix", calculate_returns = FALSE) {
                #' @description CBOE Volatility Index
                #' @return date and vix (volatility index)
                x <- fredr_series_observations(
                    series_id = id,
                    observation_start = as.Date("2000-01-01"),
                    observation_end = Sys.Date())
                
                x <- as.data.table(x)
                x <- x[, .(date, value)]
                x <- unique(x)
                setnames(x, c("date", name))

                # calculate returns
                if (calculate_returns) {
                    x <- na.omit(x)
                    x[, paste0(name, "_ret_month") := get(name) / shift(get(name), 22) - 1]
                    x[, paste0(name, "_ret_year") := get(name) / shift(get(name), 252) - 1]
                    x[, paste0(name, "_ret_week") := get(name) / shift(get(name), 5) - 1]}
                x
            }

            get_fred_vintage <- function(id = "TB3MS", name = "3_month_T_Bill") {
                #' @description 3 month T-Bill Secondary Market Rate, Discount Basis
                #' @description constructs a vintage time series — capturing how a FRED economic indicator was originally reported over time, rather than 
                #' just showing the most recent (revised) data
                start_dates <- seq.Date(as.Date("2000-01-01"), Sys.Date(), by = 365)
                end_dates <- c(start_dates[-1], Sys.Date())
                map_fun <- function(start_date, end_date) {
                x <- tryCatch({
                    fredr_series_observations(
                    series_id = id,
                    observation_start = start_date,
                    observation_end = end_date,
                    realtime_start = start_date,
                    realtime_end = end_date
                    )
                }, error = function(e) NULL)
                x
                }
                x_l <- mapply(map_fun, start_dates, end_dates, SIMPLIFY = FALSE)
                x <- rbindlist(x_l)
                x <- unique(x)
                x <- x[, .(realtime_start, value)]
                setnames(x, c("date", name))
                x
            }

            sp500 <- get_fred("SP500", "sp500", TRUE)
            oil <- get_fred("DCOILWTICO", "oil", TRUE)
            vix <- get_fred(id = "VIXCLS", name = "vix")
            t10y2y <- get_fred("T10Y2Y", "10_year_Bond")

            # series with vintage days
            tbl <- get_fred_vintage("TB3MS", "3_month_T_Bill")
            welch_goyal <- tbl[welch_goyal, on = c("date"), roll = Inf]

            macro <- Reduce(function(x, y) merge(x, y, by = "date", all.x = TRUE, all.y = FALSE),
                list(welch_goyal, vol_52_indicators, vix, sp500, oil, t10y2y))

            message("[16] Macros Done")

            return(list(prices_factors = prices,
                fundamental_factors = fundamentals,
                macro = macro))
        }
    )
)

#try1 <- get_factors$new()
#results <- try1$get_factor_data()



#view(results$prices_factors[symbol == "ABBV"])
#view(results$fundamental_factors[symbol == "ABBV"])
#view(results$macro)

#str(results$prices_factors)
#str(results$fundamental_factors)
