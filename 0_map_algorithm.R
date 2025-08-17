# extracting data from companies under map files
get_map_data <- function(map_path, latest_zip_path, data_path){
    # this function uses the map_algorithm() function for most cases and map_algorithm_exception() for the exception cases
    # to extract the daily data for the company based on the map file
    # for example, if the map file is "map_files/googl.csv", based on the date given, the get_map_data() function will extract the daily data for the companies in the file
    # @param map_path: the path of the company, e.g. "map_files/googl.csv"
    # @param latest_zip_path: the path to the latest zip file containing the map files, e.g. "/Users/arthurgoh//data/equity/usa/map_files/map_files_20250523.zip"
    # @param data_path: the path to the data folder, e.g. "/Users/arthurgoh/data/equity/usa/daily/"
    # @returns: a data.table containing the consolidated extracted DATA for the company

    example_company <- gsub(".*\\/|\\.csv$", "", map_path)
    # example_company <- company_name # e.g. "googl", "jci", "bhvn", "ritm", "cade", "alit", "lbkrv" (SPECIAL CASE) or "frph" (normal case)
    # example_company_map_path <- paste0("map_files/", example_company, ".csv") # "map_files/frph.csv"
    example_company_data_path <- paste0(data_path, example_company, ".zip") # "data/equity/usa/daily/gnrl.zip"
    latest_map_zip <- get_map_factor_companies(common_path, "map")$latest_zip  # "/Users/arthurgoh//data/equity/usa/map_files/map_files_20250523.zip"
    symbol <- example_company

    file_success <- tryCatch({ # in case I put in all upper case
        suppressWarnings(unzip(latest_map_zip, files = map_path, exdir = tempdir()))
        TRUE
    }, error = function(e) FALSE)

    if (!file_success || !file.exists(file.path(tempdir(), map_path))) {
        stop("You should use lower case for the company name")
    }

    map_data <- fread(file.path(tempdir(), map_path)) %>% as.data.table()
    last_row <- nrow(map_data) # check the number of rows in the data

    first_row_ticker <- get_daily_data(paste0(data_path, map_data[1,V2], ".zip")) # first row's ticker daily data
    second_row_ticker <- get_daily_data(paste0(data_path, map_data[2,V2], ".zip")) # second row's ticker daily data

    if (!is.null(first_row_ticker) && nrow(first_row_ticker) > 0) {
        first_row_ticker_first_date <- first_row_ticker[1, date] # first date of first row's ticker
    } else {
        first_row_ticker_first_date <- NULL
    }

    if (!is.null(second_row_ticker) && nrow(second_row_ticker) > 0) {
        second_row_ticker_first_date <- second_row_ticker[1, date] # first date of second row's ticker
    } else {
        second_row_ticker_first_date <- NULL
    }

    # output data is CALLED extracted_data: different use cases
    if (map_data[1, V2] == map_data[2, V2] && nrow(map_data) == 2){ # test: GNRL
        # print("Special Case I: Same Ticker in First and Second Row, with 2 Rows")
        # print("Nothing wrong with the company: no name changes")
        # print("Use Factor Algorithm Directly, No Map Algorithm Required")
        extracted_data <- get_daily_data(example_company_data_path) # return the data of the first row's ticker
    } else if (map_data[1, V2] == map_data[2, V2] && nrow(map_data) > 2){ # test: GOOGL
        # print("Normal Case: Same Ticker in First and Second Row")
        # print("[Common Use Case] There is a change in name for the company")
        # print("Use Map Algorithm then Factor Algorithm")
        extracted_data <- map_algorithm(map_data) # use the map algorithm to extract the data
    } else if (map_data[1,V2] != map_data[2, V2] && nrow(map_data) <= 3){ # test: ENR
        # print("Special Case II: Different Ticker in First and Second Row, with 3 or Fewer Rows")
        # print("Company Undergo Merger/Acquisition")
        # print("Use Factor Algorithm Directly, No Map Algorithm Required")
        extracted_data <- get_daily_data(example_company_data_path, start_date = as.Date(as.character(map_data[1, V1]), format = "%Y%m%d")) # return the data of the first row's ticker: ADDED A START DATE 090625
    } else if (map_data[1,V2] != map_data[2, V2] && nrow(map_data) > 3 && as.Date(as.character(map_data[1, V1]), format = "%Y%m%d") %in% c(second_row_ticker_first_date - 1, second_row_ticker_first_date, second_row_ticker_first_date + 1, second_row_ticker_first_date + 3)){ # test: RITM
        # print("Special Case III: Different Ticker in First and Second Row, with More than 3 Rows, with Second Row's Ticker First Date Same as First Row's Date")
        # print("Company Undergo Merger/Acquisition")
        # print("Use Map Algorithm then Factor Algorithm")
        # print("INSTRUCTIONS: START FROM SECOND ROW, IGNORE FIRST ROW")
        extracted_data <- map_algorithm(map_data)
    } else if(map_data[1,V2] != map_data[2, V2] && nrow(map_data) > 3 && as.Date(as.character(map_data[1, V1]), format = "%Y%m%d") %in% c(first_row_ticker_first_date -1, first_row_ticker_first_date, first_row_ticker_first_date + 1, first_row_ticker_first_date + 3)){ # test: lll
        # print("Special Case IV: Different Ticker in First and Second Row, with More than 3 Rows, with First Row's Ticker First Date Same as First Row's Date")
        # print("Company Undergo Merger/Acquisition")
        # print("Use Map Algorithm then Factor Algorithm")
        # print("INSTRUCTIONS: START FROM FIRST ROW, IGNORE SECOND ROW. Use first row's date as the start date and second row's date as the end date")
        extracted_data <- map_algorithm_exception(map_data)
    } else {
        print(paste0("Unknown Case: Please Check the Ticker: ", example_company))
        extracted_data <- NULL
    }
    return(extracted_data)
}

#=========
map_algorithm <- function(map_data){
    # the normal map algorithm that deals with most cases. this function extracts the data from tickers based on the date ranges in the map file
    # @param map_data: the data.table extracted from the map file, with columns: V1 (date), V2 (symbol), V3 (symbol_exchange_code)
    # note: some tickers do not have exchange code, so the V3 column may not exist
    # @returns: a data.table containing the extracted data for the tickers in the map file

    adjusted_map_data <- map_data %>%
        mutate(V1 = as.Date(as.character(V1), format = "%Y%m%d")) %>%
        rename(
            date = V1,
            symbol = V2) %>%
        { if ("V3" %in% names(.)) rename(., symbol_exchange_code = V3) else . } %>% # there are some tickers with no exchange code
        mutate(
            start_date = case_when(
                row_number() == 2 ~ lag(date), # if second row, the start date is the previous date
                TRUE ~ lag(date) + 1 # otherwise, the start date is the previous date + 1
            ),
            end_date = case_when(
                row_number() == nrow(.) ~ Sys.Date(), # [ASSUMED] if last row, we assume the end date is today
                TRUE ~ date # otherwise, the end date is the row's date
            )
        ) %>%
        { if ("symbol_exchange_code" %in% names(.)) # there are some tickers with no exchange code
            select(., date, symbol, start_date, end_date, symbol_exchange_code) 
        else 
            select(., date, symbol, start_date, end_date) 
        } %>%
        slice(-1) %>% select(-date) # we do not need the date column and first row because now the data shows the relationship more clearly

    extracted_data <- data.table()
    for (i in 1:nrow(adjusted_map_data)) {
        start_date <- adjusted_map_data$start_date[i]
        end_date <- adjusted_map_data$end_date[i]
        symbol <- adjusted_map_data$symbol[i]

        # get the daily data for the symbol
        daily_data <- get_daily_data(paste0(data_path, symbol, ".zip"), start_date, end_date)
        
        # bind the data to the extracted_dt
        extracted_data <- rbind(extracted_data, daily_data)    
    }

    return(extracted_data)
}


#=========
map_algorithm_exception <- function(map_data){
    # deals with the exception when the first row's ticker is different from the second row's ticker, but the first row's date is the same as the second row's ticker first date: skips the ticker on second row
    # this function extracts the data from tickers based on the date ranges in the map file, using a slightly different algorithm as compared to the map_algorithm() function
    # @param map_data: the data.table extracted from the map file, with columns: V1 (date), V2 (symbol), V3 (symbol_exchange_code)
    # note: some tickers do not have exchange code, so the V3 column may not exist
    # @returns: a data.table containing the extracted data for the tickers in the map file
    adjusted_map_data <- map_data %>%
        mutate(V1 = as.Date(as.character(V1), format = "%Y%m%d")) %>%
        rename(
            date = V1,
            symbol = V2
        ) %>%
        { if ("V3" %in% names(.)) rename(., symbol_exchange_code = V3) else . } %>% # there are some tickers with no exchange code
        mutate(
            start_date = case_when(
                row_number() == 1 ~ date, # if second row, the start date is the previous date
                row_number() == 2 ~ date,
                TRUE ~ lag(date) + 1 # otherwise, the start date is the previous date + 1
            ),
            end_date = case_when(
                row_number() == n() ~ Sys.Date(), # [ASSUMED] if last row, we assume the end date is today
                row_number() == 1 ~ lead(date), # if first row, the end date is the row's date
                TRUE ~ date # otherwise, the end date is the row's date
            )
        ) %>%
        { if ("symbol_exchange_code" %in% names(.)) 
            select(., date, symbol, start_date, end_date, symbol_exchange_code) # there are some tickers with no exchange code
        else 
            select(., date, symbol, start_date, end_date) 
        } %>%
        slice(-2) %>% select(-date) # we do not need the date column and second row because now the data shows the relationship more clearly

    extracted_data <- data.table()
    for (i in 1:nrow(adjusted_map_data)) {
        start_date <- adjusted_map_data$start_date[i]
        end_date <- adjusted_map_data$end_date[i]
        symbol <- adjusted_map_data$symbol[i]

        # get the daily data for the symbol
        daily_data <- get_daily_data(paste0(data_path, symbol, ".zip"), start_date, end_date)
        
        # bind the data to the extracted_data
        extracted_data <- rbind(extracted_data, daily_data)    
    }
    return(extracted_data)
}

#==== TEST ====
# test
# get_map_data("GNRL") # should give an error because the company name should be in lower case
# get_map_data("gnrl") # test 1 - WORKS
# get_map_data("googl") # test 2 - WORKS
# get_map_data("enr") # test 3 - works
# get_map_data("ritm") # test 4 - WORKS
# get_map_data("lll") # test 5
# get_map_data("oilu")
