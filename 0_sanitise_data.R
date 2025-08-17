#rm(list = ls(all = TRUE))
get_map_factor_companies <- function(common_path, file){
    # [factor_files] returns ALL the companies with dividends and/or splits but in their path form "...csv" OR
    # [map_files] returns ALL the companies that have gone through ticker changes in their path form "...csv"
    # and the latest zip file
    # @param common_path: the common path: should be "/Users/arthurgoh/"
    # @param file: either the "map_files" or "factor_files" folder : INPUT IS EITHER map or factor
    # @returns contains all the companies that have gone through ticker changes [for map files] OR 
    # @returns contains all the companies that have gone through dividends and/or splits [for factor files]

    #/Users/arthurgoh
    file_type <- paste0(file, "_files")
    zip_files <- list.files(path = paste0(common_path, "/data/equity/usa/", file_type), pattern = "\\.zip$", full.names = TRUE)
    file_info <- file.info(zip_files)
    latest_zip <- rownames(file_info[which.max(file_info$mtime), ]) # latest zip file
    zip_contents <- unzip(latest_zip, list = TRUE)

    list(zip_contents = zip_contents, latest_zip = latest_zip)
    }

get_factor_data <- function(file_name, latest_zip){
    # EXTRACT the DATA from the factor folder, based on the file name/path
    # @param file_name: the name of the file to read, e.g. "factor_files/cyno.csv"
    # @param latest_zip: the latest zip file containing the factor files
    # @returns a list containing the factor DATA and the symbol
    symbol <- gsub(".*\\/|\\.csv$", "", file_name)
    unzip(latest_zip, files = file_name, exdir = tempdir())
    data <- as.data.table(fread(file.path(tempdir(), file_name)))
    #file.remove(file.path(tempdir(), file_name)) # delete the file after reading: need to return TRUE

    factor <- data %>%
        select(- V4) %>%
        mutate(symbol = symbol,
                V1 = as.Date(as.character(V1), format = "%Y%m%d")) %>%
        rename(
            date = V1,
            dividend_factor = V2, # dividends
            split_factor = V3 # stock splits
        )

    #rm(data, file_info, zip_contents, file_name, latest_zip, zip_files)
    list(factor = factor, symbol = symbol)
    }


get_daily_data <- function(file_name, start_date = NULL, end_date = NULL){
    # EXTRACT the DATA from the daily folder which contains the historical prices of the company
    # accepts any date range using start_date and end_date (IN yyyymmdd format)
    # @param file_name: the name of the file to read, e.g. "data/equity/usa/daily/aapl.zip"
    # @param start_date: the start date of the data to filter, in Date format, default is NULL (earliest date in the data)
    # @param end_date: the end date of the data to filter, in Date format, default is NULL (latest date in the data)
    # @returns a data.table containing the daily data with columns: date, open, high, low, close, volume, symbol
    symbol <- gsub(".*\\/|\\.zip$", "", file_name) # get the symbol from the file name
    daily <- tryCatch({ # in the event the file does not exist, return : to catch those companies in map files that do not have daily data
        fread(file_name) %>% 
        as.data.table() %>%
        rename(
            date = V1,
            open = V2, # open
            high = V3, # high
            low = V4, # low
            close = V5, # close
            volume = V6 # volume
        ) %>%
        mutate(
            date = as.Date(gsub(" 00:00(:00)?", "", date), format = "%Y%m%d"),
            across(c(open, high, low, close), ~ as.numeric(.x) / 10000), # in deci cents : need convert
            #volume = as.integer(volume), # VERY IMPORTANT: if not volume will be very small: need force: used numeric instead bc some numbers are too big: crkn with volume of 2655579190
            volume = as.numeric(volume),
            symbol = symbol
        )}, error = function(e) {
            return(NULL)
        })
    
    if (is.null(daily)) return (NULL)

    # Dynamically assign start_date and end_date to the data's earliest and latest date if NULL
    if (is.null(start_date)) {
        start_date <- min(daily$date, na.rm = TRUE)
    }
    if (is.null(end_date)) {
        end_date <- max(daily$date, na.rm = TRUE)
    }

    # Now filter by date range
    daily <- daily %>% filter(date >= start_date & date <= end_date)

    return(daily)
    }

adjust_market_data <- function(daily, factor,
    cols_adjust = c("open", "high", "low", "close")) {
    # Mr Mislav's adjust_market_data function from UtilsData class under findata package
    # https://github.com/MislavSag/findata/blob/main/R/UtilsData.R : MODIFIED A LITTLE
    
    # Used to adjust the daily data (ONLY FOR COMPANIES WITH DIVIDENDS AND/OR SPLITS)
    # @param daily: the daily data with columns: date, open, high, low, close, volume, symbol
    # @param factor: the factor data with columns: date, dividend_factor, split_factor, symbol
    # @param cols_adjust: the columns to adjust, default is c("open", "high", "low", "close")
    # @returns a data.table containing the adjusted daily data with columns: date, open, high, low, close, volume, symbol, adj_close
    
    # checks: TEMPORARILY REMOVED TO GET THE SYMBOLS THAT HAVE PROBLEMS: task 680 failed - "Assertion on 'colnames(daily)' failed: Must have names.
    assert_names(colnames(daily), must.include = c("symbol", "date"))
    assert_names(colnames(factor),
                must.include = c("symbol", "date", "dividend_factor",
                                "split_factor"))

    # merge daily data and factor files
    daily[, date_ := as.Date(date)]
    daily <- factor[daily, on = c("symbol" = "symbol", "date" = "date_"), roll = -Inf] # no match row gets rolled by next row: means if cannot find the dividend/split factor, will use the NEXT NEAREST DATE

    # [ADJUSTED CLOSE] adjust for dividends and splits
    daily[, adj_close := as.numeric(close) * as.numeric(split_factor) * as.numeric(dividend_factor)]

    # [ADJUSTED VOLUME] adjust for splits only
    daily[, volume := as.numeric(volume) / as.numeric(split_factor)]

    # [OPEN, HIGH, LOW, CLOSE] adjust for splits only
    daily[, (cols_adjust) := lapply(.SD, function(x) as.numeric(x) * as.numeric(split_factor)),
        .SDcols = cols_adjust]
    daily[, c(2, 3, 5) := NULL] # remove price_factor, split_factor, date

    return(daily)
    }


raw_adjust_market_data <- function(daily, factor){
    # no adjustment done here. this function is used to extract ALL the raw data from the daily and factor files
    # Used to combine the daily data with factor data (ONLY FOR COMPANIES WITH DIVIDENDS AND/OR SPLITS)
    # @param daily: the daily data with columns: date, open, high, low, close, volume, symbol
    # @param factor: the factor data with columns: date, dividend_factor, split_factor, symbol
    # @param cols_adjust: the columns to adjust, default is c("open", "high", "low", "close")
    # @returns a data.table containing the adjusted daily data with columns: date, open, high, low, close, volume, symbol, adj_close
    
    # checks: TEMPORARILY REMOVED TO GET THE SYMBOLS THAT HAVE PROBLEMS: task 680 failed - "Assertion on 'colnames(daily)' failed: Must have names.
    assert_names(colnames(daily), must.include = c("symbol", "date"))
    assert_names(colnames(factor),
                must.include = c("symbol", "date", "dividend_factor",
                                "split_factor"))

    # merge daily data and factor files
    daily[, date_ := as.Date(date)]
    daily <- factor[daily, on = c("symbol" = "symbol", "date" = "date_"), roll = -Inf] # no match row gets rolled by next row: means if cannot find the dividend/split factor, will use the NEXT NEAREST DATE
    daily[, i.date := NULL] # remove the i.date column which is just a copy of date
    daily <- daily %>% mutate(
        dividend_factor = as.numeric(dividend_factor),
        split_factor = as.numeric(split_factor)
    )

    # # [ADJUSTED CLOSE] adjust for dividends and splits
    # daily[, adj_close := as.numeric(close) * as.numeric(split_factor) * as.numeric(dividend_factor)]

    # # [ADJUSTED VOLUME] adjust for splits only
    # daily[, volume := as.numeric(volume) / as.numeric(split_factor)]

    # # [OPEN, HIGH, LOW, CLOSE] adjust for splits only
    # daily[, (cols_adjust) := lapply(.SD, function(x) as.numeric(x) * as.numeric(split_factor)),
    #     .SDcols = cols_adjust]
    # daily[, c(2, 3, 5) := NULL] # remove price_factor, split_factor, date

    return(daily)
    }



#====== GRAVEYARD: TO BE DELETED ======
# get_dividend_split_companies <- function(current_path){
#     # returns ALL the companies with dividends and/or splits but in their path form "...csv"
#     # and the latest zip file
#     # @param current_path: the current working directory path
#     # @returns contains all the companies that have gone through dividends and/or splits
#     zip_files <- list.files(path = paste0(current_path, "/data/equity/usa/factor_files"), pattern = "\\.zip$", full.names = TRUE)
#     file_info <- file.info(zip_files)
#     latest_zip <- rownames(file_info[which.max(file_info$mtime), ]) # latest zip file
#     zip_contents <- unzip(latest_zip, list = TRUE)

#     list(zip_contents = zip_contents, latest_zip = latest_zip)
#     }

# test
# data <- get_sanitized_data(21) # try 21 because it is still listed
# tail(data)
# rm(data)