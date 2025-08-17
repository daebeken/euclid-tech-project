#' @description consolidating everything nicely into 1 class object
#' @note replication from Mr Mislav's UtilsData.R script from findata package: https://github.com/MislavSag/findata/blob/main/R/UtilsData.R
#' @description 26 KEY FUNCTIONS HERE
#' @seealso most important 2 functions: get_non_fundamentals_data() and get_fundamental_data()
#' @note 18 FUNCTIONS TO UNDERSTAND A COMPANY, 4 FUNCTIONS NOT AS IMPORTANT TO DESCRIBE A COMPANY, 4 FUNCTIONS TO DESCRIBE THE WHOLE EQUITY UNIVERSE
#' @note CHECK PRIVATE MEMBERS: THE PART WHERE NICO'S FUNCTIONS ARE BEING LOADED: MIGHT NEED TO CHANGE BECAUSE IT CAN CAUSE ERROR
#' @note SIMPLE TEST FOR ABOVE IS USING A NEW INSTANCE: try1 <- extract_data_FMP$new() # new instance
#' @note PROBLEM: 5 years worth of data (1255 rows)??
#' @note special use case [1]: earnings announcement, earnings surprises : some companies just IPO: so no earnings announcement
#' @note special use case [2]: dividends: some companies do not give dividends and therefore will not have columns related to dividends: need to see if that is a problem when combining multiple firms
#' @note extract_data_fmp_class.R IS THE CORE SCRIPT

#' @note all the columns to join them together: "market_capitalization_date", "income_statement_date", "income_statement_growth_date", "balance_sheet_statement_date", "balance_sheet_statement_growth_date"
#' @warning !!! HAS ABSOLUTE PATHS: NEED TO BE CAREFUL!!!

extract_data_FMP <- R6::R6Class(
    classname = "extract_data_FMP",
    private = list(
        load_dependencies = function(){
            #' @description loads the R packages used in this class
            #' @note if the packages is missing in the laptop, install the packages for them
            required_packages <- c("tidyverse", "httr", "data.table", "purrr", "parallel", "future", "future.apply", "lubridate", "jsonlite")
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

            #===== [Might need to be refined when there is error] Need to find Nico's utils and tables function =====
            # setwd("../")
            path_name <- '/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/Nico_FMP'
            source(paste0(path_name, "/utils.R"))
            source(paste0(path_name, "/tables.R"))
            message("Nico's Functions to Extract Data from FMP Have Been Imported")
            #==== Parallel Processing Set Up =====
            plan(multisession, workers = parallel::detectCores())  # safe for macOS
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
        #========================== [1] HISTORICAL MARKET CAPITALIZATION ========================================================================================================
        #' @description get the historical market capitalisation of the firm, on a daily basis
        #' @return symbol, date, market_cap
        get_historical_market_capitalization = function(company_symbol = NULL, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            #' @description recursive algorithm to get the company's historical market capitalisation
            #' @note used a recursive algorithm because FMP only allows 5 years of data to be extracted per API request; needed an algorithm that consolidates all the
            #' market capitalisation data together
            get_ipo_date = function(company_symbol = NULL, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
                #' @description inner function: only market capitalisation requires IPO date
                #' @return IPO date of the company symbol, in date data format
                endpoint <- "historical-market-capitalization"
                query_list <- list(limit = limit, from = start_date, to = end_date, year = NULL)
                temp_df <- get.endpoint.content.f(
                    path_params= company_symbol,
                    endpoint = "profile", 
                    query_list = NULL,
                    FMP_API_KEY = self$FMP_API_KEY)$ipo_date
                ipo_date = as.Date(temp_df)
                return(ipo_date)
            }

            get_market_capitalisation_recursive <- function(symbol, start_date, end_date, final_dt = data.table()) {
                #' @description recursive algorithm to get all the market capitalisation data of the company since its IPO day
                #' @return symbol, market_cap, market_capitalization_date
                if (end_date < get_ipo_date(symbol)) return(final_dt)
                
                query_list <- list(from = start_date, to = end_date, year = NULL, limit = NULL)
                temp_df <- get.endpoint.content.f(
                    path_params = symbol,
                    endpoint = "historical-market-capitalization", 
                    query_list = query_list,
                    FMP_API_KEY = self$FMP_API_KEY
                )

                if (nrow(temp_df) == 0) return(final_dt)

                historical_market_capitalization <- temp_df %>%
                    mutate(
                        market_capitalization_date = as.Date(date),
                        market_capitalization = as.numeric(market_cap)
                    ) %>%
                    select(-c(date, market_cap)) %>%
                    as.data.table()

                final_dt <- data.table::rbindlist(list(final_dt, historical_market_capitalization), fill = TRUE)

                next_end <- start_date - 1
                next_start <- next_end - 365

                get_market_capitalisation_recursive(symbol, next_start, next_end, final_dt)
            }

            final_dt <- data.table()
            ipo_date <- get_ipo_date(company_symbol)
            end_date <- Sys.Date()
            start_date <- end_date - 365
            df <- get_market_capitalisation_recursive(company_symbol, start_date, end_date, final_dt) # works
            return(df)
        },
        #========================== [2] COMPANY'S INCOME STATEMENT (QUARTERLY & ANNUALLY) =======================================================================================
        #' @description extracts the income statement of the company: THE NORMAL INCOME STATEMENT
        #' @return information contained in the income statement
        get_income_statement = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "income-statement"
            #--- Doing Quarter Period ---
            query_list <- list(period = "quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            )

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                temp_df <- temp_df %>%
                    mutate(
                        date = as.Date(date),
                        cik = as.numeric(cik),
                        filing_date = as.Date(filing_date),
                        accepted_date = as.Date(accepted_date)
                    ) %>% as.data.table()
                
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("income_statement_", cols_to_rename, "_quarter"))
            }

            #--- Doing Annual Period ---
            query_list <- list(period="annual", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            )

            if (nrow(temp_df2) == 0){
                return(NULL)
            } else {
                temp_df2 <- temp_df2 %>%
                    mutate(
                        date = as.Date(date),
                        cik = as.numeric(cik),
                        filling_date = as.Date(filing_date),
                        accepted_date = as.Date(accepted_date)
                    ) %>% as.data.table()
                
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("income_statement_",cols_to_rename, "_annual"))
            }
            
            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df,  fiscal_year)
                setkey(temp_df2, fiscal_year)
                # income_statement <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                income_statement <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                income_statement[, c("income_statement_symbol_annual", "income_statement_reported_currency_annual", "income_statement_cik_annual", "income_statement_date_annual"):= NULL]
                setnames(income_statement, old = c("income_statement_symbol_quarter", "income_statement_reported_currency_quarter", "income_statement_cik_quarter", "income_statement_date_quarter"), new = c("symbol", "income_statement_reported_currency", "income_statement_cik", "income_statement_date"))

                return(income_statement)
            } else {
               return(NULL)
            }
            
        },
        #========================== [3] COMPANY'S INCOME STATEMENT GROWTH (QUARTERLY & ANNUALLY) ==============================================================================
        #' @description extracts the income statement of the company: THE INCOME STATEMENT GROWTH!!!
        #' @return information contained in the income statement growth
        get_income_statement_growth = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "income-statement-growth"
            #--- Doing Quarter Period ---
            query_list <- list(period = "quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            # use case: when firm is new and have not reported as much
            if (nrow(temp_df) == 0){
                return(NULL)
            } else{
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("income_statement_", cols_to_rename, "_quarter"))
            }
            

            #--- Doing Annual Period ---
            query_list <- list(period="annual", limit = 1000)
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()
            
            if (nrow(temp_df2) == 0){
                return(NULL)
            } else{
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("income_statement_", cols_to_rename, "_annual"))
            }

            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df,  fiscal_year)
                setkey(temp_df2, fiscal_year)
                # income_statement_growth <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                income_statement_growth <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                income_statement_growth[, c("income_statement_symbol_annual", "income_statement_date_annual", "income_statement_period_annual"):= NULL]
                setnames(income_statement_growth, old = c("income_statement_symbol_quarter", "income_statement_date_quarter", "income_statement_period_quarter"), new = c("symbol", "income_statement_growth_date", "income_statement_growth_period"))
                return(income_statement_growth)}
            else {
                return(NULL)
            }
        },
        #========================== [4] COMPANY'S BALANCE SHEET STATEMENT (QUARTERLY & ANNUALLY) =======================================================================================
        #' @description extracts the balance sheet of the company
        #' @return information contained in the balance sheet
        get_balance_sheet_statement = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "balance-sheet-statement"
            #--- Doing Quarter Period ---
            # query_list <- list(period="quarter")
            query_list <- list(period = "quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("balance_sheet_statement_", cols_to_rename, "_quarter"))
            }

            #--- Doing Annual Period ---
            # query_list <- list(period="annual")
            query_list <- list(period="annual", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df2) == 0){
                return(NULL)
            } else {
                temp_df2[, accepted_date := as.Date(accepted_date)]
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("balance_sheet_statement_", cols_to_rename, "_annual"))
                }


            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df,  fiscal_year)
                setkey(temp_df2, fiscal_year)
                # balance_sheet_statement <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                balance_sheet_statement <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                balance_sheet_statement[, c("balance_sheet_statement_symbol_annual", "balance_sheet_statement_date_annual", "balance_sheet_statement_period_annual", "balance_sheet_statement_cik_annual", "balance_sheet_statement_reported_currency_annual"):= NULL]
                setnames(balance_sheet_statement, old = c("balance_sheet_statement_symbol_quarter", "balance_sheet_statement_date_quarter", "balance_sheet_statement_period_quarter", "fiscal_year", "balance_sheet_statement_cik_quarter"), new = c("symbol", "balance_sheet_statement_date", "balance_sheet_statement_period", "balance_sheet_statement_fiscal_year", "balance_sheet_statement_cik"))
                return(balance_sheet_statement)}
            else {
               return(NULL)
            }
        },
        #========================== [5] COMPANY'S BALANCE SHEET STATEMENT GROWTH (QUARTERLY & ANNUALLY) ==============================================================================
        #' @description extracts the balance sheet statement of the company: THE BALANCE SHEET STATEMENT GROWTH!!!
        #' @return information contained in the balance sheet statement growth
        get_balance_sheet_statement_growth = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "balance-sheet-statement-growth"
            #--- Doing Quarter Period ---
            query_list <- list(period = "quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("balance_sheet_statement_", cols_to_rename, "_quarter"))}

            #--- Doing Annual Period ---
            query_list <- list(period="annual", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()
            
            if (nrow(temp_df2) == 0){
                return(NULL)
            } else {
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("balance_sheet_statement_", cols_to_rename, "_annual"))}

            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df, fiscal_year)
                setkey(temp_df2, fiscal_year)
                # balance_sheet_statement_growth <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                balance_sheet_statement_growth <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                balance_sheet_statement_growth[, c("balance_sheet_statement_symbol_annual", "balance_sheet_statement_date_annual", "balance_sheet_statement_period_annual"):= NULL]
                setnames(balance_sheet_statement_growth, old = c("balance_sheet_statement_symbol_quarter", "balance_sheet_statement_date_quarter", "balance_sheet_statement_period_quarter", "fiscal_year"), new = c("symbol", "balance_sheet_statement_growth_date", "balance_sheet_statement_growth_period", "balance_sheet_statement_growth_fiscal_year"))
                return(balance_sheet_statement_growth)}
            else {
               return(NULL)
            }
        },
        #========================== [6] COMPANY'S CASH FLOW STATEMENT (QUARTERLY & ANNUALLY) =======================================================================================
        #' @description extracts the cash flow statement of the company
        #' @return information contained in the balance sheet
        get_cashflow_statement = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "cash-flow-statement"
            #--- Doing Quarter Period ---
            query_list <- list(period="quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("cashflow_statement_", cols_to_rename, "_quarter"))
            }

            #--- Doing Annual Period ---
            query_list <- list(period="annual", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df2) == 0){
                return(NULL)
            } else {
                temp_df2[, accepted_date := as.Date(accepted_date)]
                
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("cashflow_statement_", cols_to_rename, "_annual"))
            }

            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df,  fiscal_year)
                setkey(temp_df2, fiscal_year)
                # cashflow_statement <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                cashflow_statement <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                cashflow_statement[, c("cashflow_statement_symbol_annual", "cashflow_statement_date_annual", "cashflow_statement_period_annual", "cashflow_statement_cik_annual", "cashflow_statement_reported_currency_annual"):= NULL]
                setnames(cashflow_statement, old = c("cashflow_statement_symbol_quarter", "cashflow_statement_date_quarter", "cashflow_statement_period_quarter", "fiscal_year", "cashflow_statement_cik_quarter"), new = c("symbol", "cashflow_statement_date", "cashflow_statement_period", "cashflow_statement_fiscal_year", "cashflow_statement_cik"))
                return(cashflow_statement)
            } else {
               return(NULL)
            }
        },
        #========================== [7] COMPANY'S CASH FLOW STATEMENT GROWTH (QUARTERLY & ANNUALLY) ==============================================================================
        #' @description extracts the balance sheet statement of the company: THE CASHFLOW STATEMENT GROWTH!!!
        #' @return information contained in the balance sheet statement growth
        get_cashflow_statement_growth = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "cash-flow-statement-growth"
            #--- Doing Quarter Period ---
            query_list <- list(period = "quarter", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                # Adjusting column names for quarter
                cols_to_rename <- setdiff(names(temp_df), "fiscal_year")
                setnames(temp_df, cols_to_rename, paste0("cashflow_statement_", cols_to_rename, "_quarter"))
            }

            #--- Doing Annual Period ---
            query_list <- list(period="annual", limit = 1000) # set limit = 1000 to get as much data as we can as possible
            #' @note has calendar year
            temp_df2 <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()
            
            if (nrow(temp_df2) == 0){
                return(NULL)
            } else {
                # Adjusting column names for annual
                cols_to_rename <- setdiff(names(temp_df2), "fiscal_year")
                setnames(temp_df2, cols_to_rename, paste0("cashflow_statement_", cols_to_rename, "_annual"))
            }

            #===== Joining Both Data Tables Together =====
            if (nrow(temp_df) > 0 || nrow(temp_df2) > 0){
                setkey(temp_df,  fiscal_year)
                setkey(temp_df2, fiscal_year)
                # cashflow_statement_growth <- temp_df[temp_df2, on = "fiscal_year", nomatch = 0L, allow.cartesian = TRUE] # combining data tables with 1 to many relationship
                cashflow_statement_growth <- temp_df2[temp_df, on = "fiscal_year", allow.cartesian = TRUE] # right join (joining temp_df [quarterly] onto temp_df2 [annually])

                #===== Removing Some Unecessary Columns and Renaming Some =====
                cashflow_statement_growth[, c("cashflow_statement_symbol_annual", "cashflow_statement_date_annual", "cashflow_statement_period_annual"):= NULL]
                setnames(cashflow_statement_growth, old = c("cashflow_statement_symbol_quarter", "cashflow_statement_date_quarter", "cashflow_statement_period_quarter", "fiscal_year"), new = c("symbol", "cashflow_statement_growth_date", "cashflow_statement_growth_period", "cashflow_statement_growth_fiscal_year"))
                return(cashflow_statement_growth)
            } else {
               return(NULL)
            }
        },
        #========================== [8] EARNINGS ANNOUNCEMENT ==============================================================================
        #' @description the upcoming and past earnings announcements for firms
        #' @return date, symbol, eps, eps_estimated, time, revenue, revenue_estimated, updated_from_date (not important), fiscal_date_ending (not important)
        get_earnings_announcement = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "earnings" # changed "historical/earning_calendar" to "earnings"
            query_list <- list(limit = 1000)
            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint,
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            )

            if (nrow(temp_df) == 0) {
               return(NULL)
            } else {
               earnings_announcement <- temp_df %>% select(-c(last_updated)) %>%
                mutate(
                    earnings_announcement_date = as.Date(date),
                    symbol = as.character(symbol),
                    eps = as.numeric(eps_actual),
                    eps_estimated = as.numeric(eps_estimated),
                    revenue = as.numeric(revenue_actual),
                    revenue_estimated = as.numeric(revenue_estimated))
            
                if (!is.null(start_date) && !is.null(end_date)) {
                    earnings_announcement <- earnings_announcement %>% filter(earnings_announcement_date >= as.Date(start_date) & earnings_announcement_date <= as.Date(end_date))
                }
                
                earnings_announcement <- earnings_announcement %>% as.data.table()
                #===== Removing Some Unecessary Columns and Renaming Some =====
                earnings_announcement[, c("date"):= NULL]
                setnames(earnings_announcement, old = c("eps", "eps_estimated", "revenue", "revenue_estimated"), new = c("earnings_announcement_eps", "earnings_announcement_eps_estimated", "earnings_announcement_revenue", "earnings_announcement_revenue_estimated"))
                earnings_announcement[, c("eps_actual", "revenue_actual"):= NULL]
                
                return(earnings_announcement)
            }
        },
        #========================== [9] EARNINGS SURPRISE ==============================================================================
        #' @description compares the earnings results with the ESTIMATED earnings of the firm
        #' @return date, symbol, actual_earning_result, estimated_earnings
        #' NO NEED EARNING SURPRISE BECAUSE IT IS UNDER EARNINGS
        
        # get_earnings_surprise = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
        #     endpoint <- "earnings-surprises"
        #     query_list <- list(limit = limit, from = start_date, to = end_date)
        #     temp_df <- get.endpoint.content.f(
        #         path_params= company_symbol,
        #         endpoint = endpoint, 
        #         query_list = query_list,
        #         FMP_API_KEY = self$FMP_API_KEY
        #     )

        #     if (nrow(temp_df) == 0) {
        #        return(NULL)
        #     } else {
        #         earnings_surprise <- temp_df %>%
        #             mutate(
        #                 earnings_surprise_date = as.Date(date),
        #                 symbol = as.character(symbol),
        #                 actual_earning_result = as.numeric(actual_earning_result),
        #                 estimated_earning = as.numeric(estimated_earning))
                
        #         if (!is.null(start_date) && !is.null(end_date)) {
        #         earnings_surprise <- earnings_surprise %>% filter(earnings_announcement_date >= as.Date(start_date) & earnings_announcement_date <= as.Date(end_date))
        #         }

        #         earnings_surprise <- earnings_surprise %>% as.data.table()
        #         #===== Removing Some Unecessary Columns and Renaming Some =====
        #         earnings_surprise[, c("date"):= NULL]
        #         setnames(earnings_surprise, old = c("actual_earning_result", "estimated_earning"), new = c("earnings_surprise_actual_eps", "earnings_surprise_estimated_eps"))
        #         return(earnings_surprise)
        #     }
        # },
        #========================== [10] INCLUDE IN SUPPLEMENTARY INFORMATION: EARNINGS CALL TRANSCRIPT ==============================================================================
        #' @description extracts the earnings call transcript in the whole year
        #' @param IMPORTANT: need to have the year parameter
        #' @note the new API documentation requires: symbol, year and quarter: and you only get 1 quarter at a time
        #' @note used a for loop to get all the quarters
        #' @example get_earnings_call_transcript("AAPL", year = 2024)
        #' @return symbol, quarter, year, date, transcript content
        get_earnings_call_transcript = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            # endpoint <- "batch_earning_call_transcript" # did not use this anymore because of the new api documentation: requires the quarter as a parameter as well
            endpoint <- "earning-call-transcript"
            temp_df <- data.table()

            for (i in 1:4){
                query_list <- list(year = year, quarter = i)
                transcript_df <- get.endpoint.content.f(
                    path_params= company_symbol,
                    endpoint = endpoint, 
                    query_list = query_list,
                    FMP_API_KEY = self$FMP_API_KEY)
                
                temp_df <- rbindlist(list(temp_df, transcript_df), fill = TRUE, use.names = TRUE)
            }

            if (nrow(temp_df) == 0) {
               return(NULL)
            } else {
                earnings_call_transcript <- temp_df %>%
                    mutate(
                        symbol = as.character(symbol),
                        period = as.character(period),
                        # quarter = as.numeric(quarter),
                        year = as.numeric(year),
                        date = as.Date(date),
                        transcript_content = as.character(content)) %>% 
                        as.data.table()
                
                #===== Removing Some Unecessary Columns and Renaming Some =====
                # setnames(earnings_call_transcript, old = c("content"), new = c("earnings_call_transcript"))
                exclude_cols <- c("symbol")
                cols_to_rename <- setdiff(names(earnings_call_transcript), exclude_cols)
                new_names <- paste0("earnings_call_transcript_", cols_to_rename)
                setnames(earnings_call_transcript, old = cols_to_rename, new = new_names)
                return(earnings_call_transcript)
            }
        },
        #========================== [11] DIVIDENDS ==============================================================================
        #' @description extracts the dividends paid by the company
        #' @return symbol, date, adj_dividend, dividend, dividend_record_date, dividend_payment_date, dividend_declaration_date
        get_dividends = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "dividends"
            query_list <- list(limit = 1000)

            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            )

            if (nrow(temp_df) == 0){
                return(NULL)
            } else {
                dividends <- temp_df %>%
                    mutate(
                        symbol = as.character(symbol),
                        ex_dividend_date = as.Date(date),
                        adj_dividend = as.numeric(adj_dividend),
                        dividend = as.numeric(dividend),
                        yield = as.numeric(yield),
                        record_date = as.Date(record_date),
                        payment_date = as.Date(payment_date),
                        declaration_date = as.Date(declaration_date)
                    ) %>% dplyr::rename(
                        adj_dividend_amount = adj_dividend,
                        dividend_amount = dividend,
                        dividend_yield = yield,
                        dividend_record_date = record_date,
                        dividend_payment_date = payment_date,
                        dividend_declaration_date = declaration_date
                    ) %>% select(-c(frequency, date)) %>% as.data.table()
                return(dividends)  
            }
        },
        #========================== [12] IPO DATE ==============================================================================
        #' @description extracts the IPO date of the company
        #' @return symbol, cik, ipo date
        #' @note this is a little different from Nico's function because it only wants the IPO date which is inside company outlook
        get_ipo_date = function(company_symbol = NULL, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            #' @return IPO date of the company symbol, in date data format
            endpoint <- "profile"
            query_list <- NULL
            temp_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY)$ipo_date
            ipo_date = as.Date(temp_df)
            return(ipo_date)
        },
        #========================== [13] COMPANY'S FINANCIAL RATIOS (QUARTERLY) ==============================================================================
        #' @description extracts some financial ratios
        get_financial_ratios_quarterly = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "ratios"
            query_list <- list(limit = 1000, period="quarter")

            financial_ratios_quarterly <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(financial_ratios_quarterly) == 0) {
               return(NULL)
            } else {
                exclude_cols <- c("symbol", "fiscal_year", "period")
                cols_to_rename <- setdiff(names(financial_ratios_quarterly), exclude_cols)
                new_names <- paste0("financial_ratios_quarterly_", cols_to_rename)
                setnames(financial_ratios_quarterly, old = cols_to_rename, new = new_names)

                return(financial_ratios_quarterly)
            }
        },
        #========================== [14] COMPANY'S FINANCIAL RATIOS (ANNUALLY) ==============================================================================
        #' @description extracts some financial ratios
        get_financial_ratios_annually = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "ratios"
            query_list <- list(limit = 1000, period="annual")

            financial_ratios_annually <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(financial_ratios_annually) == 0) {
               return(NULL)
            } else {
                financial_ratios_annually <- financial_ratios_annually %>% select(-period)

                exclude_cols <- c("symbol", "fiscal_year")
                cols_to_rename <- setdiff(names(financial_ratios_annually), exclude_cols)
                new_names <- paste0("financial_ratios_annually_", cols_to_rename)
                setnames(financial_ratios_annually, old = cols_to_rename, new = new_names)

                return(financial_ratios_annually)
            }
        },
        #========================== [15] COMPANY'S FINANCIAL METRICS (QUARTERLY) ==============================================================================
        #' @description extracts some financial metrics
        get_financial_metrics_quarterly = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "key-metrics"
            query_list <- list(period="quarter", limit = 1000)

            financial_metrics_quarterly <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(financial_metrics_quarterly) == 0) {
               return(NULL)
            } else {
                exclude_cols <- c("symbol", "fiscal_year", "period")
                cols_to_rename <- setdiff(names(financial_metrics_quarterly), exclude_cols)
                new_names <- paste0("financial_metrics_quarterly_", cols_to_rename)
                setnames(financial_metrics_quarterly, old = cols_to_rename, new = new_names)

                return(financial_metrics_quarterly)
            }
        },
        #========================== [16] COMPANY'S FINANCIAL METRICS (ANNUALLY) ==============================================================================
        #' @description extracts some financial metrics
        get_financial_metrics_annually = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "key-metrics"
            query_list <- list(period="annual", limit = 1000)

            get_financial_metrics_annually <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(get_financial_metrics_annually) == 0) {
               return(NULL)
            } else {
                get_financial_metrics_annually <- get_financial_metrics_annually %>% select(-period)
                exclude_cols <- c("symbol", "fiscal_year")
                cols_to_rename <- setdiff(names(get_financial_metrics_annually), exclude_cols)
                new_names <- paste0("financial_metrics_annually_", cols_to_rename)
                setnames(get_financial_metrics_annually, old = cols_to_rename, new = new_names)

                return(get_financial_metrics_annually)
            }
        },
        #========================== [17] COMPANY'S FINANCIAL GROWTH (QUARTERLY) ==============================================================================
        #' @description extracts some financial growth
        get_financial_growth_quarterly = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "financial-growth"
            query_list <- list(period="quarter", limit = 1000)

            financial_growth_quarterly <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(financial_growth_quarterly) == 0) {
               return(NULL)
            } else {
                exclude_cols <- c("symbol", "fiscal_year", "period")
                cols_to_rename <- setdiff(names(financial_growth_quarterly), exclude_cols)
                new_names <- paste0("financial_growth_quarterly_", cols_to_rename)
                setnames(financial_growth_quarterly, old = cols_to_rename, new = new_names)

                return(financial_growth_quarterly)
            }
        },
        #========================== [18] COMPANY'S FINANCIAL GROWTH (ANNUALLY) ==============================================================================
        #' @description extracts some financial growth
        get_financial_growth_annually = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "financial-growth"
            query_list <- list(period="annual", limit = 1000)

            financial_growth_annually <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(financial_growth_annually) == 0) {
               return(NULL)
            } else {
                financial_growth_annually <- financial_growth_annually %>% select(-period)
                exclude_cols <- c("symbol", "fiscal_year")
                cols_to_rename <- setdiff(names(financial_growth_annually), exclude_cols)
                new_names <- paste0("financial_growth_annually_", cols_to_rename)
                setnames(financial_growth_annually, old = cols_to_rename, new = new_names)

                return(financial_growth_annually)
            }
        },
        #========================== [18] INCLUDE IN SUPPLEMENTARY INFORMATION: PRICE TARGET ==============================================================================
        #' @description extracts the price target of the firm from multiple agencies
        #' @return symbol, published date, price target, adjusted price target, analyst company
        #' @note this is a little different from Nico's function because it is not inside the github (above)
        #' @note if want more historical price targets, need to adjust the page number as well: example: list(page = 0, limit = 1000)
        get_price_target = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "price-target-news"
            pages <- 0:50

            results <- future_lapply(pages, function(n){
                query_list <- list(page = n, limit = 1000)
                cleaned_df <- get.endpoint.content.f(
                    path_params= company_symbol,
                    endpoint = endpoint, 
                    query_list = query_list,
                    FMP_API_KEY = self$FMP_API_KEY
                )
            })
            
            cleaned_df <- rbindlist(results, fill = TRUE, use.names = TRUE) %>% as.data.table()

            price_target <- data.table(symbol = as.character(cleaned_df$symbol), published_date = as.Date(cleaned_df$published_date), price_target = as.numeric(cleaned_df$price_target),
                adjusted_price_target = as.numeric(cleaned_df$adj_price_target), price_when_posted = as.numeric(cleaned_df$price_when_posted) ,analyst_company = as.character(cleaned_df$analyst_company))
            
            if (nrow(price_target) == 0){
                return(NULL)
            } else {
                exclude_cols <- c("symbol")
                cols_to_rename <- setdiff(names(price_target), exclude_cols)
                new_names <- paste0("price_target_", cols_to_rename)
                setnames(price_target, old = cols_to_rename, new = new_names)
                
                return(price_target)
            }
        },
        #========================== [20] INCLUDE IN SUPPLEMENTARY INFORMATION: UPGRADES/DOWNGRADES OF RATINGS ==============================================================================
        #' @description extracts all the upgrade downgrade news 
        get_upgrades_downgrades = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "grades-news"
            pages <- 0:50

            results <- future_lapply(pages, function(n){
                query_list <- list(page = n, limit = 1000)
                cleaned_df <- get.endpoint.content.f(
                    path_params= company_symbol,
                    endpoint = endpoint, 
                    query_list = query_list,
                    FMP_API_KEY = self$FMP_API_KEY
                )
            })
            
            cleaned_df <- rbindlist(results, fill = TRUE, use.names = TRUE) %>% as.data.table()
            
            upgrades_downgrades <- data.table(symbol = as.character(cleaned_df$symbol), published_date = as.Date(cleaned_df$published_date), grading_company = as.character(cleaned_df$grading_company),
                previous_grade = as.character(cleaned_df$previous_grade), new_grade = as.character(cleaned_df$new_grade), action = as.character(cleaned_df$action))
            
            exclude_cols <- c("symbol")
            cols_to_rename <- setdiff(names(upgrades_downgrades), exclude_cols)
            new_names <- paste0("upgrades_downgrades_", cols_to_rename)
            setnames(upgrades_downgrades, old = cols_to_rename, new = new_names)
            
            return(upgrades_downgrades)
        },
        #========================== [21] HISTORICAL RATINGS ==============================================================================
        get_company_ratings = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            #' @note max limit is 10000, cannot change the date as well
            try1 <- self
            ipo_date <- try1$get_ipo_date(company_symbol)
            endpoint <- "ratings-historical"
            query_list <- list(limit = 10000)

            company_ratings <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            if (nrow(company_ratings) == 0) {
               return(NULL)
            } else {
                exclude_cols <- c("symbol")
                cols_to_rename <- setdiff(names(company_ratings), exclude_cols)
                new_names <- paste0("company_ratings_", cols_to_rename)
                setnames(company_ratings, old = cols_to_rename, new = new_names)

                return(company_ratings)
            }
        },
        #========================== [22] STOCK GRADES ANALYST CONSENSUS ==============================================================================
        get_stock_grades_consensus = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "grades-historical"
            query_list <- list(limit = 10000)

            cleaned_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            stock_grades_consensus <- data.table(symbol = as.character(cleaned_df$symbol), date = as.Date(cleaned_df$date), strong_buy_analyst = as.numeric(cleaned_df$analyst_ratings_strong_buy),
                buy_analyst = as.numeric(cleaned_df$analyst_ratings_buy), hold_analyst = as.numeric(cleaned_df$analyst_ratings_hold), sell_analyst = as.numeric(cleaned_df$analyst_ratings_sell),
                strong_sell_analyst = as.numeric(cleaned_df$analyst_ratings_strong_sell))
            
            exclude_cols <- c("symbol")
            cols_to_rename <- setdiff(names(stock_grades_consensus), exclude_cols)
            new_names <- paste0("stock_grades_consensus_", cols_to_rename)
            setnames(stock_grades_consensus, old = cols_to_rename, new = new_names)
            
            return(stock_grades_consensus)
        },
        #========================== [23] BENEFICIAL OWNERSHIP ==========================
        get_beneficial_ownership = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            endpoint <- "acquisition-of-beneficial-ownership"
            query_list <- list(limit = 10000)

            cleaned_df <- get.endpoint.content.f(
                path_params= company_symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY
            ) %>% as.data.table()

            beneficial_ownership <- data.table(
                cik = as.numeric(cleaned_df$cik), symbol = as.character(cleaned_df$symbol), filing_date = as.Date(cleaned_df$filing_date), accepted_date = as.Date(cleaned_df$accepted_date),
                cusip = as.numeric(cleaned_df$cusip), name_of_reporting_person = as.character(cleaned_df$name_of_reporting_person), citizenship_or_organization_pace = as.character(cleaned_df$citizenship_or_place_of_organization),
                sole_voting_power = as.numeric(cleaned_df$sole_voting_power), shared_voting_power = as.numeric(cleaned_df$shared_voting_power), sole_dispositive_power = as.numeric(cleaned_df$sole_dispositive_power),
                shared_dispositive_power = as.numeric(cleaned_df$shared_dispositive_power), amount_beneficially_owned = as.numeric(cleaned_df$amount_beneficially_owned), percent_of_class = as.numeric(cleaned_df$percent_of_class),
                type_of_reporting_person = as.character(cleaned_df$type_of_reporting_person), url = as.character(cleaned_df$url)
            )

            beneficial_ownership[, c("cik", "cusip"):= NULL]
            exclude_cols <- c("symbol")
            cols_to_rename <- setdiff(names(beneficial_ownership), exclude_cols)
            new_names <- paste0("beneficial_ownership_", cols_to_rename)
            setnames(beneficial_ownership, old = cols_to_rename, new = new_names)

            return(beneficial_ownership)
        },
        #========================== [24] HELPER FUNCTION: STOCK LIST SYMBOLS ==========================
        #' @description extracts all companies ticker symbols available in Financial Modeling Prep
        #' @return symbol, name, price, exchange, exchange short name, type
        #' @note gives either: stock/etf/trust/fund
        #' @note TAKES A LONG TIME (approx 15 seconds) BECAUSE THERE ARE 85668 SYMBOLS

        get_all_tickers = function(){
            res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/stock-list?apikey=', self$FMP_API_KEY))
            res_content <- content(res, as = "text", encoding = "UTF-8")
            all_tickers <- fromJSON(res_content) %>% as.data.table() %>% rename(company_name = companyName)

            return(all_tickers)
        },
        #========================== [25] HELPER FUNCTION: STOCK LIST SYMBOLS ==========================
        #' @description extracts all companies whose symbol has changed
        #' @return date, name, old_symbol, new_symbol
        
        get_tickers_change = function(){
            res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/symbol-change?limit=10000&apikey=', self$FMP_API_KEY))
            res_content <- content(res, as = "text", encoding = "UTF-8")
            tickers_change <- fromJSON(res_content) %>% as.data.table() %>% rename(
                company_name = companyName,
                old_symbol = oldSymbol,
                new_symbol = newSymbol)

            return(tickers_change)
        },
        #========================== [26] HELPER FUNCTION: ALL DELISTED COMPANIES ==========================
        #' @description extracts all companies that have delisted
        #' @return symbol, company_name, exchange, ipo_date, delisted_date
        
        get_delisted_companies = function(){
            endpoint <- "delisted-companies"
            pages <- 0:2000
            limit <- 10000 # max 100 records per request

            results <- future_lapply(pages, function(n){
                res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/', endpoint, '?page=', n, '&limit=', limit, '&apikey=', self$FMP_API_KEY))
                res_content <- content(res, as = "text", encoding = "UTF-8")
                delisted_companies <- fromJSON(res_content)
            })

            cleaned_df <- rbindlist(results, fill = TRUE, use.names = TRUE) %>% as.data.table()
            delisted_companies <- cleaned_df %>% mutate(ipoDate = as.Date(ipoDate), delistedDate = as.Date(delistedDate)) %>% rename(
                company_name = companyName,
                ipo_date = ipoDate,
                delisted_date = delistedDate)

            return(delisted_companies)
            # return(cleaned_df)
        },
        #========================== [27] HELPER FUNCTION: ALL AVAILABLE TRADED COMPANIES ==========================
        #' @description extracts all companies that are currently listed: 66839 companies as of 210725
        #' @return symbol, name, price, exchange, exchange_short_name, type
        
        get_traded_companies = function(){
            endpoint <- "actively-trading-list"
            res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/', endpoint,'?apikey=', self$FMP_API_KEY))
            res_content <- content(res, as = "text", encoding = "UTF-8")
            traded_companies <- fromJSON(res_content) %>% as.data.table()
            traded_companies <- traded_companies %>% rename(company_name = name)

            return(traded_companies)
        },
        #========================== [28] HELPER FUNCTION: COMPANIES IN INDEX ==========================
        get_index_companies = function(index = "sp500"){
            #' @description extract companies in index funds
            #' @param index: can be sp500/nasdaq/dowjones
            
            endpoint <- paste0(index, "-constituent")
            res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/', endpoint,'?apikey=', self$FMP_API_KEY))
            res_content <- content(res, as = "text", encoding = "UTF-8")
            index_companies <- fromJSON(res_content) %>% as.data.table()
            index_companies <- index_companies %>% rename(company_name = name,
                subsector = subSector,
                headquarter = headQuarter,
                date_first_added = dateFirstAdded) %>% mutate(date_first_added = as.Date(date_first_added), cik = as.numeric(cik)) #founded = as.numeric(founded)
            return(index_companies)
        },
        #========================== [29] HELPER FUNCTION: COMPANY PROFILES ==========================
        get_company_profiles = function(symbol){
            #' @description get the company profile
            #' @return basic information about the company
            endpoint <- "profile"
            query_list <- NULL
            temp_df <- get.endpoint.content.f(
                path_params= symbol,
                endpoint = endpoint, 
                query_list = query_list,
                FMP_API_KEY = self$FMP_API_KEY)
            return(temp_df)
        },
        #===================================================!!! FUNCTION TO CONSOLIDATE FUNDAMENTALS TOGETHER !!!====================================================================
        get_fundamental_data = function(symbol){
            #' @description extracts only the fundamentals of a SINGLE company
            #' @return items from balance sheet, income statement, cashflow statement, finanical growth, financial metrics, financial ratios
            
            try1 <- self
            # get data
            balance_sheet_dt <- try1$get_balance_sheet_statement(symbol)
            income_statement_dt <- try1$get_income_statement(symbol)
            cashflow_statement_dt <- try1$get_cashflow_statement(symbol)
            financial_growth_dt <- try1$get_financial_growth_quarterly(symbol)
            financial_metrics_dt <- try1$get_financial_metrics_quarterly(symbol)
            financial_ratios_dt <- try1$get_financial_ratios_quarterly(symbol)

            #' @note for companies that recently IPO and do not have much data
            dt_list <- list(balance_sheet_dt, income_statement_dt, cashflow_statement_dt, financial_growth_dt, financial_metrics_dt, financial_ratios_dt)
            
            # case when the company is new and does not have any reported data
            #' @example company: EMA
            if (any(sapply(dt_list, function(dt) is.null(dt) || nrow(dt) == 0))) {
                return(NULL)}

            # set keys
            setkey(balance_sheet_dt, symbol, balance_sheet_statement_date)
            setkey(income_statement_dt, symbol, income_statement_date)
            setkey(cashflow_statement_dt, symbol, cashflow_statement_date)
            setkey(financial_growth_dt, symbol, financial_growth_quarterly_date)
            setkey(financial_metrics_dt, symbol, financial_metrics_quarterly_date)
            setkey(financial_ratios_dt, symbol, financial_ratios_quarterly_date)

            # merging them together (inner join)
            try1_bs <- copy(balance_sheet_dt)
            try1_is <- copy(income_statement_dt)
            try1_cs <- copy(cashflow_statement_dt)
            try1_fg <- copy(financial_growth_dt)
            try1_fm <- copy(financial_metrics_dt)
            try1_fr <- copy(financial_ratios_dt)

            special_symbols <- c("EQV")
            if (symbol %in% special_symbols){
                #' @note to deal with special cases such as EQV that have wrong reported dates
                #' @example in special symbols like EQV
                setnames(try1_bs, old = c("balance_sheet_statement_period", "balance_sheet_statement_fiscal_year"), new = c("report_period", "report_year"))
                setnames(try1_is, old = c("income_statement_period_quarter", "fiscal_year"), new = c("report_period", "report_year"))
                setnames(try1_cs, old = c("cashflow_statement_period", "cashflow_statement_fiscal_year"), new = c("report_period", "report_year"))
                setnames(try1_fg, old = c("period", "fiscal_year"), new = c("report_period", "report_year"))
                setnames(try1_fm, old = c("period", "fiscal_year"), new = c("report_period", "report_year"))
                setnames(try1_fr, old = c("period", "fiscal_year"), new = c("report_period", "report_year"))

                setkey(try1_bs, symbol, report_period, report_year)
                setkey(try1_is, symbol, report_period, report_year)
                setkey(try1_cs, symbol, report_period, report_year)
                setkey(try1_fg, symbol, report_period, report_year)
                setkey(try1_fm, symbol, report_period, report_year)
                setkey(try1_fr, symbol, report_period, report_year)

                fundamentals <- merge(try1_bs, try1_is, by = c("symbol", "report_year", "report_period"), all = FALSE)
                fundamentals <- merge(fundamentals, try1_cs, by = c("symbol", "report_year", "report_period"), all = FALSE)
                fundamentals <- merge(fundamentals, try1_fg, by = c("symbol", "report_year", "report_period"), all = FALSE)
                fundamentals <- merge(fundamentals, try1_fm, by = c("symbol", "report_year", "report_period"), all = FALSE)
                fundamentals <- merge(fundamentals, try1_fr, by = c("symbol", "report_year", "report_period"), all = FALSE)

                # change back: to fit together with the rest
                setnames(fundamentals, old = c("report_period", "report_year"), new = c("period", "fiscal_year"))
                setnames(fundamentals, "balance_sheet_statement_date", "report_date")
                cols_to_drop <- c("income_statement_date", "cashflow_statement_date", "financial_growth_quarterly_date", "financial_metrics_quarterly_date", "financial_ratios_quarterly_date",
                    "income_statement_period_annual", "cashflow_statement_cash_at_end_of_period_annual", "cashflow_statement_cash_at_beginning_of_period_annual",
                    "cashflow_statement_cash_at_end_of_period_quarter", "cashflow_statement_cash_at_beginning_of_period_quarter")
                fundamentals[, (cols_to_drop) := NULL]
                # sort the data table
                setcolorder(fundamentals, c("symbol", "report_date", "fiscal_year", "period", setdiff(names(fundamentals), c("symbol", "report_date", "fiscal_year", "period"))))
                setkey(fundamentals, symbol, report_date)
                return(fundamentals)
            }

            #' @note for normal cases
            setnames(try1_bs, "balance_sheet_statement_date", "report_date")
            setnames(try1_is, "income_statement_date", "report_date")
            setnames(try1_cs, "cashflow_statement_date", "report_date")
            setnames(try1_fg, "financial_growth_quarterly_date", "report_date")
            setnames(try1_fm, "financial_metrics_quarterly_date", "report_date")
            setnames(try1_fr, "financial_ratios_quarterly_date", "report_date")

            setkey(try1_bs, symbol, report_date)
            setkey(try1_is, symbol, report_date)
            setkey(try1_cs, symbol, report_date)
            setkey(try1_fg, symbol, report_date)
            setkey(try1_fm, symbol, report_date)
            setkey(try1_fr, symbol, report_date)

            fundamentals <- merge(try1_bs, try1_is, by = c("symbol", "report_date"), all = FALSE)
            fundamentals <- merge(fundamentals, try1_cs, by = c("symbol", "report_date"), all = FALSE)
            fundamentals <- merge(fundamentals, try1_fg, by = c("symbol", "report_date"), all = FALSE)
            fundamentals <- merge(fundamentals, try1_fm, by = c("symbol", "report_date"), all = FALSE)
            fundamentals <- merge(fundamentals, try1_fr, by = c("symbol", "report_date"), all = FALSE)

            cols_to_drop <- setdiff(names(fundamentals)[grepl("fiscal_year", names(fundamentals), ignore.case = FALSE)], "fiscal_year")
            fundamentals[, (cols_to_drop) := NULL]
            keep_col <- "fiscal_year.x"
            setnames(fundamentals, old = keep_col, new = "fiscal_year")
            drop_cols <- setdiff(grep("^fiscal_year\\.", names(fundamentals), value = TRUE), keep_col)
            fundamentals[, (drop_cols) := NULL]

            cols_to_drop <- setdiff(names(fundamentals)[grepl("period", names(fundamentals), ignore.case = FALSE)], "period")
            fundamentals[, (cols_to_drop) := NULL]
            # sort the data table
            setcolorder(fundamentals, c("symbol", "report_date", "fiscal_year", "period", setdiff(names(fundamentals), c("symbol", "report_date", "fiscal_year", "period"))))
            setkey(fundamentals, symbol, report_date)
            # fundamentals <- fundamentals %>% arrange(desc(report_date))
            return(fundamentals)
        },
        #===================================================!!! FUNCTION TO CONSOLIDATE EVERYTHING TOGETHER !!!====================================================================
        get_non_fundamentals_data = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
            #' @note did not add earnings call transcript, price target, upgrades downgrades, beneficial ownership, delisted companies, all companies, traded companies
            #' @returns market capitalisation, company ratings, dividends, ipo date and stock grades consensus
            try1 <- self
            #======= extract relevant information =========
            try2 <- try1$get_historical_market_capitalization(company_symbol)
            try11 <- try1$get_company_ratings(company_symbol)
            try12 <- try1$get_dividends(company_symbol)
            try13 <- try1$get_ipo_date(company_symbol)
            try20 <- try1$get_stock_grades_consensus(company_symbol)

            print("Information Extraction Completed")
            Sys.sleep(2)
            #======= setting the joining up =========
            setkey(try2, market_capitalization_date)

            #' @note to deal with special cases such as EQV that have wrong reported dates
            #' @example in special symbols like EQV
            valid_try11 <- !is.null(try11) && is.data.table(try11) && nrow(try11) > 0
            valid_try12 <- !is.null(try12) && is.data.table(try12) && nrow(try12) > 0 # uses ex dividend date instead of record date; some companies do not give dividends
            valid_try20 <- !is.null(try20) && is.data.table(try20) && nrow(try20) > 0

            if (valid_try11 || valid_try12 || valid_try20) {
                if (valid_try11) {setkey(try11, company_ratings_date)}
                if (valid_try12) {setkey(try12, ex_dividend_date)}
                if (valid_try20) {setkey(try20, stock_grades_consensus_date)}
            } else {
                message(paste0(company_symbol, " does not have such data. Possible because they recently IPO"))
                return(NULL)
            }

            print("Setting of Keys Completed")
            Sys.sleep(2)
            #======= joining data tables together =========
            if (valid_try11){
                try11 <- try11[, company_ratings_report_date := company_ratings_date]
                result <- try11[try2, on = .(company_ratings_date = market_capitalization_date), roll = Inf]
                result[, market_capitalization_date := company_ratings_date]
                result[, company_ratings_date := NULL]
            }
            
            # some companies may not have dividends
            if (!is.null(try12) && nrow(try12) > 0){
                try12 <- try12[, ex_dividend_report_date := ex_dividend_date]
                if (exists("result") && is.data.table(result)){
                    #' @note to deal with special cases such as EQV that have wrong reported dates
                    #' @example in special symbols like EQV
                    result <- try12[result, on = .(ex_dividend_date = market_capitalization_date), roll = Inf]
                    result[, market_capitalization_date := ex_dividend_date]
                    result[, ex_dividend_date := NULL]
                } else {
                    #' @note normal case
                   result <- try2 # market capitalization data
                   result <- result[, market_capitalization_reported_date := market_capitalization_date]
                   result <- try12[result, on = .(ex_dividend_date = market_capitalization_reported_date), roll = Inf]
                   result[, ex_dividend_date := NULL]
                }
                
            }

            # Broadcast IPO date to all rows (assumes only one IPO date)
            result[, ipo_date := try13] # try13 only because try13 is just a date

            if (valid_try20){
                try20 <- try20[, stock_grades_consensus_report_date := stock_grades_consensus_date]
                result <- try20[result, on = .(stock_grades_consensus_date = market_capitalization_date), roll = Inf]
                result[, market_capitalization_date := stock_grades_consensus_date]
                result[, stock_grades_consensus_date := NULL]
            }
            
            # multiple columns with i.symbol: need to remove
            cols_to_drop <- setdiff(names(result)[grepl("symbol", names(result), ignore.case = FALSE)], "symbol")
            result[, (cols_to_drop) := NULL]

            cols_to_drop <- setdiff(names(result)[grepl("fiscal_year", names(result), ignore.case = FALSE)], "fiscal_year")
            result[, (cols_to_drop) := NULL]

            cols_to_drop <- setdiff(names(result)[grepl("period", names(result), ignore.case = FALSE)], "period")
            result[, (cols_to_drop) := NULL]

            print("Information Joining Completed")
            Sys.sleep(2)
            #======= DONE =========
            setkey(result, symbol, market_capitalization_date)
            setcolorder(result, c("symbol", "market_capitalization_date","market_capitalization", setdiff(names(result), c("symbol", "market_capitalization_date","market_capitalization"))))
            result <- result %>% arrange(desc(market_capitalization_date))
            result[, symbol := toupper(company_symbol)] # force all rows in symbol column to have at least something
            return(result)
        }
    )
)
# ================================================================ END OF CODE, TEST NOW ================================================================
# try1 <- extract_data_FMP$new() # new instance
# df1 <- try1$get_fundamental_data("AAPL")
# # df2 <- try1$get_non_fundamentals_data("AAPL")
# # view(df)

# try1$get_company_profiles("AAPL")

# # view(try1$get_stock_grades_consensus("AAPL"))
# try99 <- try1$get_index_companies("sp500")
# symbols <- try99$symbol[1:100]
# num_companies <- 1:length(symbols)

# # try100 <- try1$get_company_profiles("AAPL")
# # symbols[1]

# results <- mclapply(num_companies, function(n){
#     instance <- extract_data_FMP$new()
#     symbol <- symbols[n]
#     company_profile <- try1$get_company_profiles(symbol)
#     return(company_profile)
# }, mc.cores = 16)

# cleaned_df <- rbindlist(results, fill = TRUE, use.names = TRUE) %>% as.data.table()
# view(cleaned_df)
# #=========================================
# # see which one in US
# symbols_us_profiles <- httr::GET(paste0("https://financialmodelingprep.com/api/v4/profile/all?apikey=",instance_1$FMP_API_KEY))
# csv_text <- rawToChar(symbols_us_profiles$content)
# us_profiles <- as.data.table(fread(csv_text))

# us_profiles <- us_profiles[country == "US" & isEtf == FALSE & isFund == FALSE] # remove all that are type funds and etfs
# setnames(us_profiles, tolower(colnames(us_profiles)))

# # == works: add here once it starts working ==
# try2 <- try1$get_historical_market_capitalization("AAPL") # OK: full market capitalisation: from IPO date
# try3 <- try1$get_income_statement("AAPL") # OK
# try4 <- try1$get_income_statement_growth("AAPL") # OK
# try5 <- try1$get_balance_sheet_statement("AAPL") #OK
# try6 <- try1$get_balance_sheet_statement_growth("AAPL") #OK
# try7 <- try1$get_cashflow_statement("AAPL") #OK
# try8 <- try1$get_cashflow_statement_growth("AAPL") #OK
# try9 <- try1$get_earnings_announcement("AAPL") #OK
# try10 <- try1$get_earnings_surprise("AAPL") # NO NEED
# try11 <- try1$get_earnings_call_transcript("AAPL", year = 2006) # SEPARATE FROM THE MAIN WORKFLOW: EARNIGN TRANSCRIPT: OK
# try12 <- try1$get_dividends("AAPL") #OK
# try13 <- try1$get_ipo_date("AAPL") #OK
# try14 <- try1$get_financial_ratios_quarterly("AAPL") #OK
# try15 <- try1$get_financial_ratios_annually("AAPL") #OK
# try16 <- try1$get_financial_metrics_quarterly("AAPL") # OK
# try17 <- try1$get_financial_metrics_annually("AAPL") # OK
# try18 <- try1$get_financial_growth_quarterly("AAPL") # OK
# try19 <- try1$get_financial_growth_annually("AAPL") # OK
# try20 <- try1$get_price_target("AAPL") # SEPARATE FROM THE MAIN WORKFLOW: PRICE TARGET # OK
# try21 <- try1$get_upgrades_downgrades("AAPL") # SEPARATE FROM THE MAIN WORKFLOW: UPGRADES DOWNGRADES (GRADING ACTIONS) #OK
# try20 <- try1$get_company_ratings("AAPL") # OK
# try21 <- try1$get_stock_grades_consensus("AAPL") # OK
# try22 <- try1$get_beneficial_ownership("AAPL") # SEPARATE FROM THE MAIN WORKFLOW: BENEFICIAL OWNERSHIP #OK
# try23 <- try1$get_all_tickers() # SEPARATE FROM THE MAIN WORKFLOW: ALL TICKERS FMP HAS TO OFFER #OK
# try24 <- try1$get_tickers_change() # SEPARATE FROM THE MAIN WORKFLOW: ALL TICKERS THAT WENT THROUGH TICKERS CHANGE # OK
# try25 <- try1$get_delisted_companies() # SEPARATE FROM THE MAIN WORKFLOW: ALL TICKERS THAT HAVE DELISTED #OK
# try26 <- try1$get_traded_companies() # SEPARATE FROM THE MAIN WORKFLOW: ALL TICKERS ARE AVAILABLE TRADED COMPANIES # OK
# # == test ==



# #================= FUNCTION TO CONSOLIDATE EVERYTHING TOGETHER =================
# get_full_data = function(company_symbol, start_date = NULL, end_date = NULL, limit = NULL, year = NULL){
#     #' @note did not add earnings call transcript: explode the memory with the transcript
#     try1 <- extract_data_FMP$new() # new instance
#     #======= extract relevant information =========
#     try2 <- try1$get_historical_market_capitalization(company_symbol)
#     try3 <- try1$get_income_statement(company_symbol)
#     try4 <- try1$get_income_statement_growth(company_symbol)
#     try5 <- try1$get_balance_sheet_statement(company_symbol)
#     try6 <- try1$get_balance_sheet_statement_growth(company_symbol)
#     try7 <- try1$get_cashflow_statement(company_symbol)
#     try8 <- try1$get_cashflow_statement_growth(company_symbol)
#     try9 <- try1$get_earnings_announcement(company_symbol)
#     try10 <- try1$get_earnings_surprise(company_symbol)
#     try11 <- try1$get_company_ratings(company_symbol)
#     try12 <- try1$get_dividends(company_symbol)
#     try13 <- try1$get_ipo_date(company_symbol)
#     try14 <- try1$get_financial_ratios_quarterly(company_symbol)
#     try15 <- try1$get_financial_ratios_annually(company_symbol)
#     try16 <- try1$get_financial_metrics_quarterly(company_symbol)
#     try17 <- try1$get_financial_metrics_annually(company_symbol)
#     try18 <- try1$get_financial_growth_quarterly(company_symbol)
#     try19 <- try1$get_financial_growth_annually(company_symbol)
#     try20 <- try1$get_stock_grades_consensus(company_symbol)

#     print("Information Extraction Completed")
#     Sys.sleep(2)
#     #======= setting the joining up =========
#     setkey(try2, market_capitalization_date)
#     setkey(try3, income_statement_date)
#     setkey(try4, income_statement_growth_date)
#     setkey(try5, balance_sheet_statement_date)
#     setkey(try6, balance_sheet_statement_growth_date)
#     setkey(try7, cashflow_statement_date)
#     setkey(try8, cashflow_statement_growth_date)
#     setkey(try9, earnings_announcement_date)
#     setkey(try10, earnings_surprise_date)
#     setkey(try11, company_ratings_date)
#     setkey(try12, ex_dividend_date) # uses ex dividend date instead of record date
#     setkey(try14, financial_ratios_quarterly_date)
#     setkey(try15, financial_ratios_annually_date)
#     setkey(try16, financial_metrics_quarterly_date)
#     setkey(try17, financial_metrics_annually_date)
#     setkey(try18, financial_growth_quarterly_date)
#     setkey(try19, financial_growth_annually_date)
#     setkey(try20, stock_grades_consensus_date)



#     print("Setting of Keys Completed")
#     Sys.sleep(2)
#     #======= joining data tables together =========
#     try3 <- try3[, income_statement_report_date := income_statement_date]
#     result <- try3[try2, on = .(income_statement_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := income_statement_date]
#     result[, income_statement_date := NULL]

#     try4 <- try4[, income_statement_growth_report_date := income_statement_growth_date]
#     result <- try4[result, on = .(income_statement_growth_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := income_statement_growth_date]
#     result[, income_statement_growth_date := NULL]

#     try5 <- try5[, balance_sheet_report_date := balance_sheet_statement_date]
#     result <- try5[result, on = .(balance_sheet_statement_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := balance_sheet_statement_date]
#     result[, balance_sheet_statement_date := NULL]

#     try6 <- try6[, balance_sheet_growth_report_date := balance_sheet_statement_growth_date]
#     result <- try6[result, on = .(balance_sheet_statement_growth_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := balance_sheet_statement_growth_date]
#     result[, balance_sheet_statement_growth_date := NULL]

#     try7 <- try7[, cashflow_report_date := cashflow_statement_date]
#     result <- try7[result, on = .(cashflow_statement_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := cashflow_statement_date]
#     result[, cashflow_statement_date := NULL]

#     try8 <- try8[, cashflow_growth_report_date := cashflow_statement_growth_date]
#     result <- try8[result, on = .(cashflow_statement_growth_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := cashflow_statement_growth_date]
#     result[, cashflow_statement_growth_date := NULL]

#     try9 <- try9[, earnings_announcement_report_date := earnings_announcement_date]
#     result <- try9[result, on = .(earnings_announcement_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := earnings_announcement_date]
#     result[, earnings_announcement_date := NULL]

#     try10 <- try10[, earnings_surprise_report_date := earnings_surprise_date]
#     result <- try10[result, on = .(earnings_surprise_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := earnings_surprise_date]
#     result[, earnings_surprise_date := NULL]

#     try11 <- try11[, company_ratings_report_date := company_ratings_date]
#     result <- try11[result, on = .(company_ratings_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := company_ratings_date]
#     result[, company_ratings_date := NULL]

#     try12 <- try12[, ex_dividend_report_date := ex_dividend_date]
#     result <- try12[result, on = .(ex_dividend_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := ex_dividend_date]
#     result[, ex_dividend_date := NULL]

#     # Broadcast IPO date to all rows (assumes only one IPO date)
#     result[, ipo_date := try13$ipo_date]

#     try14 <- try14[, financial_ratios_quarterly_report_date := financial_ratios_quarterly_date]
#     result <- try14[result, on = .(financial_ratios_quarterly_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_ratios_quarterly_date]
#     result[, financial_ratios_quarterly_date := NULL]

#     try15 <- try15[, financial_ratios_annually_report_date := financial_ratios_annually_date]
#     result <- try15[result, on = .(financial_ratios_annually_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_ratios_annually_date]
#     result[, financial_ratios_annually_date := NULL]

#     try16 <- try16[, financial_metrics_quarterly_report_date := financial_metrics_quarterly_date]
#     result <- try16[result, on = .(financial_metrics_quarterly_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_metrics_quarterly_date]
#     result[, financial_metrics_quarterly_date := NULL]

#     try17 <- try17[, financial_metrics_annually_report_date := financial_metrics_annually_date]
#     result <- try17[result, on = .(financial_metrics_annually_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_metrics_annually_date]
#     result[, financial_metrics_annually_date := NULL]

#     try18 <- try18[, financial_growth_quarterly_report_date := financial_growth_quarterly_date]
#     result <- try18[result, on = .(financial_growth_quarterly_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_growth_quarterly_date]
#     result[, financial_growth_quarterly_date := NULL]

#     try19 <- try19[, financial_growth_annually_report_date := financial_growth_annually_date]
#     result <- try19[result, on = .(financial_growth_annually_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := financial_growth_annually_date]
#     result[, financial_growth_annually_date := NULL]

#     try20 <- try20[, stock_grades_consensus_report_date := stock_grades_consensus_date]
#     result <- try20[result, on = .(stock_grades_consensus_date = market_capitalization_date), roll = Inf]
#     result[, market_capitalization_date := stock_grades_consensus_date]
#     result[, stock_grades_consensus_date := NULL]

#     # multiple columns with i.symbol: need to remove
#     cols_to_drop <- setdiff(names(result)[grepl("symbol", names(result), ignore.case = FALSE)], "symbol")
#     result[, (cols_to_drop) := NULL]

#     cols_to_drop <- setdiff(names(result)[grepl("fiscal_year", names(result), ignore.case = FALSE)], "fiscal_year")
#     result[, (cols_to_drop) := NULL]

#     cols_to_drop <- setdiff(names(result)[grepl("period", names(result), ignore.case = FALSE)], "period")
#     result[, (cols_to_drop) := NULL]

#     print("Information Joining Completed")
#     Sys.sleep(2)
#     #======= DONE =========
#     return(result)
# }

# dt <- get_full_data("AAPL")
# View(dt)
# colnames(dt)



# get.endpoint.content.f(
#     path_params= "AAPL",
#     endpoint = "income-statement", 
#     query_list = "quarter",
#     FMP_API_KEY = "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"
# )

# endpoint <- "earnings-calendar" # changed "historical/earning_calendar" to "earnings-calendar"
# query_list <- list(limit = 1000, from = "1980-01-01", to = Sys.Date())
# temp_df <- get.endpoint.content.f(
#     path_params= "AAPL",
#     endpoint = "earnings-calendar",
#     query_list = query_list,
#     FMP_API_KEY = "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"
# )


# get.endpoint.content.f(
#     path_params= "AAPL",
#     endpoint = "ratios", 
#     query_list = list(limit = 1000, period = "quarter"),
#     FMP_API_KEY = "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"
# ) %>% as.data.table() %>% view()


# require(jsonlite)

# get_all_tickers = function(){
#     endpoint <- "stock-list"
#     request_urls <- paste0('https://financialmodelingprep.com/stable/', endpoint, '?apikey=', "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH")
#     all_tickers <- get_request_content(request_urls) %>% as.data.table()

#     return(all_tickers)
# }

# FMP_API_KEY <- "u8SUx4V4hh3A5HcqOOAODL21nyYoqXcH"
# res <- httr::GET(url = paste0('https://financialmodelingprep.com/stable/stock-list?apikey=', FMP_API_KEY))
# res_content <- content(res, as = "text", encoding = "UTF-8")
# parsed_data <- fromJSON(res_content) %>% as.data.table() %>% rename(company_name = companyName)
