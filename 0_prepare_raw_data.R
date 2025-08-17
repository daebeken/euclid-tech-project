prepare_raw_data <- function(){
    # prepare_data is a function that consolidates all the data into a single data table and saves it to a database
    # ALMOST exactly the same as 2_prepare_data but this function uses raw_adjust_market_data() instead of adjust_market_data()
    # @returns a data table containing the consolidated data: companies with dividends/splits & companies with no dividends/splits
    # @returns a SAVED database connection to the raw_stocks_daily.duckdb database
    require(tidyverse)
    require(data.table)
    require(foreach)
    require(doFuture)
    require(parallel)
    require(forcats)
    require(tictoc)
    require(duckdb) # database management
    require(checkmate)
    options(warn = -1) # suppress warnings

    current_path <- getwd()
    common_path <- gsub("Library.*", "", current_path)
    data_path <- paste0(common_path, "data/equity/usa/daily/")
    source(paste0(current_path, "/0_sanitise_data.R")) # use previous R file
    source(paste0(current_path, "/0_map_algorithm.R")) # use previous R file
    source(paste0(current_path, "/1_separate_companies.R")) # use previous R file

    #====== Separate companies into factor and historical, get their associated paths ======
    all_companies <- separate_companies(common_path, data_path) # get the factor and historical companies: 1363.049 seconds
    print("Separation of Companies Done.")
    Sys.sleep(1)

    factor_companies <- all_companies$only_factor_companies # companies in factor & historical, not in map [apply factor algorithm, no map algorithm]
    map_companies <- all_companies$only_map_companies # companies in map & historical, not in factor [apply map algorithm, no factor algorithm]
    map_factor_companies <- all_companies$map_factor_companies # companies in both map & factor, in historical [apply both algorithms]
    historical_companies <- all_companies$only_historical_companies # companies in historical, NOT IN factor & map [no algorithm applied]

    factor_companies_path <- paste0("factor_files/", factor_companies, ".csv") # WITH THE PATH: "factor_files/cyno.csv" : to replace company_files and file_name
    factor_companies_daily_path <- paste0(common_path, "/data/equity/usa/daily/", factor_companies, ".zip") # path to the factor files
    
    #map_companies_path <- paste0("map_files/", map_companies, ".csv") # WITH THE PATH: "map_files/cyno.csv" : to replace company_files and file_name
    map_companies_daily_path <- paste0(common_path, "/data/equity/usa/daily/", map_companies, ".zip") # path to the map files

    map_factor_companies_map_path <- paste0("map_files/", map_factor_companies, ".csv")
    map_factor_companies_factor_path <- paste0("factor_files/", map_factor_companies, ".csv")
    map_factor_companies_daily_path <- paste0(common_path, "/data/equity/usa/daily/", map_factor_companies, ".zip") # path to the map factor files
    latest_map_zip <- get_map_factor_companies(common_path, file = "map")$latest_zip # "/Users/arthurgoh//data/equity/usa/map_files/map_files_20250523.zip"
    
    historical_companies_daily_path <- paste0(common_path, "/data/equity/usa/daily/", historical_companies, ".zip") # path to the historical files
    latest_factor_zip <- get_map_factor_companies(common_path, file = "factor")$latest_zip # "/Users/arthurgoh//data/equity/usa/factor_files/factor_files_20250523.zip"

    num_cores <- parallel::detectCores()
    cl <- parallel::makeCluster(num_cores)
    plan(cluster, workers = cl)
    registerDoFuture()

    
    print("Preparing Companies in Map Factor Data...") # 889.137 seconds
    tic <- tic()
    df_map_factor <- foreach(i = seq_along(map_factor_companies), .combine = rbind, .packages = c("data.table", "checkmate", "tidyverse"),
                                .export = c("map_factor_companies", 
                                    "map_factor_companies_map_path", 
                                    "map_factor_companies_factor_path", 
                                    "latest_map_zip", 
                                    "latest_factor_zip",
                                    "current_path",
                                    "data_path",
                                    "common_path", 
                                    "get_map_data", 
                                    "get_factor_data", 
                                    "adjust_market_data")) %dopar% {
        map_factor_name <- map_factor_companies[i] # test: "cyno"
        map_file_path <- map_factor_companies_map_path[i] # test: "map_files/cyno.csv"
        factor_file_name <- map_factor_companies_factor_path[i] # test: "factor_files/cyno.csv"
        #daily_file_name <- map_factor_companies_daily_path[i] # test: "/Users/arthurgoh//data/equity/usa/daily/cyno.zip"
        
        map_data <- get_map_data(map_file_path, latest_map_zip, data_path) # get the consolidated data
        map_data$symbol <- map_factor_name
        factor_data <- get_factor_data(factor_file_name, latest_factor_zip) # get the factor data
        combined <- raw_adjust_market_data(daily = map_data, factor = factor_data$factor) # adjust the market data with the factor data
    }
    print("Finished Task of Companies in Map Factor Data...")
    toc <- toc()
    print(paste0("Total Time Taken to Finish Extracting Data for Companies in Map Factor Data: ", round(toc$toc-toc$tic,3), " seconds"))
    Sys.sleep(1)
    
    #seq_along(factor_companies)
    # [LONG EXECUTION] ABOUT 35 MINUTES
    print("Preparing Companies in Factor Data...") # 1607.597 seconds
    tic <- tic()
    df_factor <- foreach(i = seq_along(factor_companies), .combine = rbind, .packages = c("data.table", "checkmate", "tidyverse")) %dopar% {
        factor_name <- factor_companies[i] # test: "cyno"
        file_name <- factor_companies_path[i] # test: "factor_files/cyno.csv"
        daily_file_name <- factor_companies_daily_path[i] # test: "/Users/arthurgoh//data/equity/usa/factor_files/cyno.zip"
        factor_data <- get_factor_data(file_name, latest_factor_zip) # get the factor data
        daily_data <- get_daily_data(daily_file_name) # get the daily data
        combined <- raw_adjust_market_data(daily = daily_data, factor = factor_data$factor) # adjust the market data with the factor data
    }
    print("Finished Task of Companies in Factor Data...")
    toc <- toc()
    print(paste0("Total Time Taken to Finish Extracting Data for Companies in Factor Data: ", round(toc$toc-toc$tic,3), " seconds"))
    Sys.sleep(1)

    print("Preparing Companies in Map Data...") # 10.167 seconds
    tic <- tic()
    df_map <- foreach(i = seq_along(map_companies), .combine = rbind, .packages = c("data.table", "checkmate", "tidyverse")) %dopar% {
    map_name <- map_companies[i] # test: "oih"
    #file_name <- map_companies_path[i] # test: "map_files/oih.csv"
    daily_file_name <- map_companies_daily_path[i] # test: "/Users/arthurgoh//data/equity/usa/daily/oih.zip"
    daily_data <- get_daily_data(daily_file_name) # get the daily data
    daily_data[, symbol := map_name] # Add the symbol column
    setcolorder(daily_data, c(1, ncol(daily_data), 2:(ncol(daily_data)-1))) # Reorder: move symbol to 2nd column
    daily_data$dividend_factor <- 1 # since map data does not have dividend file, we add it on our own
    daily_data$split_factor <- 1 # since map data does not have factor file, we add it on our own
    setcolorder(daily_data, c("date", "dividend_factor", "split_factor", "symbol", "open", "high", "low", "close", "volume"))
    daily_data
    }
    print("Finished Task of Companies in Map Data...")
    toc <- toc()
    print(paste0("Total Time Taken to Finish Extracting Data for Companies in Map Data: ", round(toc$toc-toc$tic,3), " seconds"))
    Sys.sleep(1)

    print("Preparing Companies in Historical Data...") # 174.738 sec
    tic <- tic()
    df_historical <- foreach(i = seq_along(historical_companies), .combine = rbind, .packages = c("data.table", "checkmate", "tidyverse")) %dopar% {
        historical_name <- historical_companies[i] # test: "cyno"
        historical_file_name <- historical_companies_daily_path[i] # test: "/Users/arthurgoh//data/equity/usa/daily/cyno.zip"
        historical_data <- get_daily_data(historical_file_name) # get the historical data
        historical_data[, symbol := historical_name]
        setcolorder(historical_data, c(1, ncol(historical_data), 2:(ncol(historical_data)-1)))
        historical_data$dividend_factor <- 1 # since map data does not have dividend file, we add it on our own
        historical_data$split_factor <- 1 # since map data does not have factor file, we add it on our own
        setcolorder(historical_data, c("date", "dividend_factor", "split_factor", "symbol", "open", "high", "low", "close", "volume"))
        historical_data
    }
    print("Finished Task of Companies in Historical Data...")
    toc <- toc()
    print(paste0("Total Time Taken to Finish Extracting Data for Companies in Historical Data: ", round(toc$toc-toc$tic,3), " seconds"))
    Sys.sleep(1)

    df_combined <- rbind(df_factor, df_map, df_map_factor ,df_historical, fill = TRUE)

    parallel::stopCluster(cl)
    
    print("Loading into Database Now...")
    con <- dbConnect(duckdb::duckdb(), dbdir = "raw_stocks_daily.duckdb", read_only = FALSE)
    dbWriteTable(con, "raw_stocks_daily", df_combined, overwrite = TRUE)
    print("Data is Saved")
    dbDisconnect(con)
    return(list(df_factor, df_map, df_map_factor ,df_historical, df_combined)) # return the combined data frame
    }


df <- prepare_raw_data() # run the function to prepare the data

# need look at the data in the following order: df_map_factor, df_factor, df_map, df_historical, then df_combined
# df_factor <- df[[1]]
# df_map <- df[[2]]
# df_map_factor <- df[[3]]
# df_historical <- df[[4]]
# df_combined <- df[[5]]

# df_factor %>% head(10)
# df_map_factor %>% head(10)
# df_map %>% head(10)
# df_historical %>% head(10)
# df_combined %>% head(10)

# df_factor %>% filter(symbol == "aapl") %>% head(10)
