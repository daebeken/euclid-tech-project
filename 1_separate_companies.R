separate_companies <- function(common_path, data_path){
    # separate the companies into 4 different categories:
    # [1] only_factor_companies: companies in factor & historical data, not in map [apply factor algorithm, no map algorithm]
    # [2] only_map_companies: companies in map & historical data, not in factor [apply map algorithm, no factor algorithm]
    # [3] map_factor_companies: companies in both map & factor, in historical data [apply both algorithms]
    # [4] only_historical_companies: companies in historical data, NOT IN factor & map [no algorithm applied]

    # @param common_path: the common path to the data folder: e.g. "/Users/arthurgoh/"
    # @param data_path: the path to the data folder: e.g. "/Users/arthurgoh/data/equity/usa/daily/"
    # @returns only_factor_companies is a vector of companies that are in both factor and historical data
    # @returns only_map_companies is a vector of companies that are in both map and historical data
    # @returns map_factor_companies is a vector of companies that are in both map and factor, and in historical data
    # @returns only_historical_companies is a vector of companies that are only in historical data
    # @returns: ONLY THE COMPANY NAME, NOT THE PATH: example:  "webme", "wecpra", "wecpracl", "weet", "wefc"     

    tic <- tic()
    factors <- get_map_factor_companies(common_path, file = "factor") # to get the dividend and split companies
    original_factor_companies <- gsub("factor_files/|\\.csv", "", factors$zip_contents$Name) # remove .csv: left with the company names
    factor_companies <- original_factor_companies[!grepl("[0-9]", original_factor_companies)] # remove those with numbers

    map_company <- get_map_factor_companies(common_path, file = "map") # to get the companies with ticker changes
    original_map_companies <- gsub("map_files/|\\.csv", "", map_company$zip_contents$Name) # remove .csv: left with the company names
    map_companies <- original_map_companies[!grepl("[0-9]", original_map_companies)] # remove those with numbers

    # historical data of companies
    historical_companies <- gsub("^.*//(.*)\\.zip$", "\\1", list.files(data_path, pattern = "\\.zip$", full.names = TRUE))

    factors_firms_in_factor_not_in_historical <- setdiff(factor_companies, historical_companies) # check which companies are missing in DAILY FILES (companies in factor file but not in historical file): REMOVE THESE COMPANIES FROM THE FACTOR FILES (PRIVATE FIRM)
    factor_companies <- factor_companies[!factor_companies %in% factors_firms_in_factor_not_in_historical] # now, all companies in factor files are in historical files

    map_firms_in_map_not_in_historical <- setdiff(map_companies, historical_companies) # check which companies are missing in DAILY FILES (companies in map file but not in historical file): REMOVE THESE COMPANIES FROM THE MAP FILES (PRIVATE FIRM)
    map_companies <- map_companies[!map_companies %in% map_firms_in_map_not_in_historical] # now, all companies in map files are in historical files


    # factor_companies, map_companies, historical_companies
    only_factor_companies <- setdiff(factor_companies, map_companies) # companies in factor & historical, not in map [apply factor algorithm, no map algorithm]
    only_map_companies <- setdiff(map_companies, factor_companies) # companies in map & historical, not in factor [apply map algorithm, no factor algorithm]
    map_factor_companies <- intersect(map_companies, factor_companies) # companies in both map & factor, in historical [apply both algorithms]
    only_historical_companies <- setdiff(historical_companies, c(factor_companies, map_companies)) # companies in historical, NOT IN factor & map [no algorithm applied]

    print("Initial Separation Done.")
    Sys.sleep(1)
    print("Starting Secondary Separation...")
    Sys.sleep(2)

    map_zip_file <- map_company$latest_zip # "/Users/arthurgoh//data/equity/usa/map_files/map_files_20250523.zip"

    num_cores <- parallel::detectCores()
    cl <- parallel::makeCluster(num_cores)
    plan(cluster, workers = cl)
    registerDoFuture()

    # LONG EXECUTION: ABOUT 23 MINUTES
    df <- foreach(i = seq_along(map_factor_companies), .combine = rbind, .packages = c("data.table", "checkmate", "tidyverse")) %dopar% {
        map_name <- map_factor_companies[i] # test: "oih"
        file_name <- paste0("map_files/", map_name, ".csv") # test: "map_files/oih.csv"
        #symbol <- gsub(".*\\/|\\.csv$", "", file_name) # test: "oih"
        unzip(map_zip_file, files = file_name, exdir = tempdir())
        data <- as.data.table(fread(file.path(tempdir(), file_name)))
        if (nrow(data) <= 2) {
            return(NULL) # skip if the data has less than or equal to 2 rows: bc the company did not change name
        }
        return(map_name)
    }

    parallel::stopCluster(cl)

    to_be_added_to_only_factor <- map_factor_companies[!map_factor_companies %in% df]
    only_factor_companies <- c(only_factor_companies, to_be_added_to_only_factor) # add the companies that are not in the map to the only factor companies
    map_factor_companies <- c(df)
    toc <- toc()
    print(paste0("Total Time Taken: ", round(toc$toc - toc$tic, 3), " seconds"))
    print("Secondary Separation Done.")
    Sys.sleep(1)

    #========== Check ==========
    if (length(only_factor_companies) + length(only_map_companies) + length(map_factor_companies) + length(only_historical_companies) != length(historical_companies)) {
        stop("[MISMATCH] The total number of companies in only_factor_companies, only_map_companies, map_factor_companies and only_historical_companies does not match the historical companies count.") # need to go debug if this happens
    }
    else{
        print("[MATCH] The total number of companies in only_factor_companies, only_map_companies, map_factor_companies and only_historical_companies MATCHES the historical companies count.")
        return(list(
            only_factor_companies = only_factor_companies, # companies in factor & historical, not in map [apply factor algorithm, no map algorithm]
            only_map_companies = only_map_companies, # companies in map & historical, not in factor [apply map algorithm, no factor algorithm]
            map_factor_companies = map_factor_companies, # companies in both map & factor, in historical [apply both algorithms]
            only_historical_companies = only_historical_companies # companies in historical, NOT IN factor & map [no algorithm applied]
        ))
        }

    }




# current_path <- getwd()
# common_path <- gsub("Library.*", "", current_path)
# data_path <- paste0(common_path, "data/equity/usa/daily/")
# source(paste0(current_path, "/0_sanitise_data.R")) # use previous R file
# source(paste0(current_path, "/1_separate_companies.R")) # use previous R file
# totality <- separate_companies(common_path, data_path)
# totality$only_factor_companies # companies in factor & historical, not in map [apply factor algorithm, no map algorithm]
# totality$only_map_companies # companies in map & historical, not in factor [apply map algorithm, no factor algorithm]
# totality$map_factor_companies # companies in both map & factor, in historical [apply both algorithms]
# totality$only_historical_companies # companies in historical, NOT IN factor & map [no algorithm applied]
