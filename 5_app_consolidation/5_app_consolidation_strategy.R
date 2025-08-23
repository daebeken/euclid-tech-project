# setwd("~") # reset working directory
# # for (i in 2:4) {
# #     common_path <- "/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate"
# #     source(file.path(paste0(common_path, "/4.", i, "_test_exuber_Nico.R")))
# # }
# 
# common_path <- "/Users/arthurgoh/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/5_app_consolidation"
# 
# exuber_portfolio_xts <- readRDS(paste0(common_path, "/exuber_portfolio_xts.rds"))
# tvgarch_strat_1_portfolio_xts <- readRDS(paste0(common_path, "/tvgarch_strat_1_portfolio_xts.rds"))
# tvgarch_strat_2_portfolio_xts <- readRDS(paste0(common_path, "/tvgarch_strat_2_portfolio_xts.rds"))
# theft_portfolio_xts <- readRDS(paste0(common_path, "/theft_portfolio_xts.rds"))
# frac_diff_portfolio_xts <- readRDS(paste0(common_path, "/frac_diff_portfolio_xts.rds"))
# setwd("~/Library/Mobile Documents/com~apple~CloudDocs/1_Euclid_Tech_Internship/zz_bristol_gate/5_app_consolidation")
# exuber_portfolio_xts <- readRDS('data/exuber_portfolio_xts.rds')
# tvgarch_strat_1_portfolio_xts <- readRDS("data/tvgarch_strat_1_portfolio_xts.rds")
# tvgarch_strat_2_portfolio_xts <- readRDS("data/tvgarch_strat_2_portfolio_xts.rds")
# theft_portfolio_xts <- readRDS("data/theft_portfolio_xts.rds")
# frac_diff_portfolio_xts <- readRDS("data/frac_diff_portfolio_xts.rds")
require(shiny)
require(shinyjs)
require(bslib)
require(data.table)
require(xts)
require(zoo)
require(DT)
require(waiter)
require(PerformanceAnalytics)

exuber_portfolio_xts <- readRDS('exuber_portfolio_xts.rds')
tvgarch_strat_1_portfolio_xts <- readRDS("tvgarch_strat_1_portfolio_xts.rds")
tvgarch_strat_2_portfolio_xts <- readRDS("tvgarch_strat_2_portfolio_xts.rds")
theft_portfolio_xts <- readRDS("theft_portfolio_xts.rds")
frac_diff_portfolio_xts <- readRDS("frac_diff_portfolio_xts.rds")

exuber_portfolio_dt <- as.data.table(exuber_portfolio_xts)
tvgarch_strat_1_portfolio_dt <- as.data.table(tvgarch_strat_1_portfolio_xts)
tvgarch_strat_2_portfolio_dt <- as.data.table(tvgarch_strat_2_portfolio_xts)
theft_portfolio_dt <- as.data.table(theft_portfolio_xts)
frac_diff_portfolio_dt <- as.data.table(frac_diff_portfolio_xts)

setorder(exuber_portfolio_dt, -index)
setorder(tvgarch_strat_1_portfolio_dt, -index)
setorder(tvgarch_strat_2_portfolio_dt, -index)
setorder(theft_portfolio_dt, -index)
setorder(frac_diff_portfolio_dt, -index)

#'=================================================================================================================

tables <- list(
    "exuber portfolio" = exuber_portfolio_dt,
    "tvgarch (strat 1)" = tvgarch_strat_1_portfolio_dt,
    "tvgarch (strat 2)" = tvgarch_strat_2_portfolio_dt,
    "theft portfolio" = theft_portfolio_dt,
    "frac diff portfolio" = frac_diff_portfolio_dt
)


sanitize_id <- function(x) gsub("\\W+", "_", x)


performance_metrics_table <- function(portfolio_xts, scale = 52, col = NULL) {
    # pick a single series (first column by default)
    x <- if (is.null(col)) portfolio_xts[, 1, drop = FALSE] else portfolio_xts[, col, drop = FALSE]
    
    # compute
    cum_ret <- Return.cumulative(x)
    ann_ret <- Return.annualized(x, scale = scale)
    ann_sharpe <- SharpeRatio.annualized(x, scale = scale)
    min_dd <- min(Drawdowns(x), na.rm = TRUE)   # most negative drawdown
    ann_sd <- PerformanceAnalytics::sd.annualized(x, scale = scale)
    
    # formatters
    pct <- function(v) sprintf("%.2f%%", 100 * as.numeric(v))
    num <- function(v) sprintf("%.2f",   as.numeric(v))
    
    data.table(
        `Performance Measure` = c(
            "Cumulative Returns",
            "Annualized Returns",
            "Annualized Sharpe Ratio",
            "Minimum Drawdown",
            "Annualized Standard Deviation"
        ),
        Value = c(
            pct(cum_ret),
            pct(ann_ret),
            num(ann_sharpe),
            pct(min_dd),
            pct(ann_sd)
        )
    )
}

render_pa_plot <- function(expr, h = 320) {
    renderPlot({
        op <- par(no.readonly = TRUE); on.exit(par(op))
        # tighter plot layout
        par(mar = c(3, 3.2, 1.6, 1),    # bottom, left, top, right
            oma = c(0, 0, 0, 0),
            mgp = c(2, 0.6, 0),         # axis title/label spacing
            tcl = -0.2,
            cex = 0.9)                  # shrink overall text a bit
        expr
    }, height = h, res = 120)
}

# ============================= UI =============================
ui <- bslib::page_navbar(
    title = "Research Panels",
    id = "nav",
    header = autoWaiter(),
    !!!lapply(names(tables), function(key) {
        id <- sanitize_id(key)
        bslib::nav_panel(
            key,
            card(
                card_header(paste0(key, " Strategy")),
                div(
                    style = "max-height: 800px; overflow-y: auto; padding-right: 10px;",
                    p("Performance Chart"),
                    plotOutput(outputId = paste0("plot1_", id), width = "100%", height = "320px"),
                    div(style = "height: 100px;"),
                    plotOutput(outputId = paste0("plot2_", id), width = "100%", height = "320px"),
                    div(style = "height: 100px;"),
                    plotOutput(outputId = paste0("plot3_", id), width = "100%", height = "320px"),
                    div(style = "height: 100px;"),
                    p("Performance Summary"),
                    DT::DTOutput(outputId = paste0("metrics_", id)),
                    div(style = "height: 100px;"),
                    p("Backtesting Result [in data table]"),
                    DT::DTOutput(outputId = paste0("tbl_",  id))
                )
            )
        )
    })
)

# ============================= SERVER =============================
server <- function(input, output, session) {
    # Map panel names to their xts series used for charts.PerformanceSummary()
    # Ensure names match those in `tables`
    series_xts <- list(
        "exuber portfolio" = exuber_portfolio_xts,
        "tvgarch (strat 1)" = tvgarch_strat_1_portfolio_xts,
        "tvgarch (strat 2)" = tvgarch_strat_2_portfolio_xts,
        "theft portfolio" = theft_portfolio_xts,
        "frac diff portfolio" = frac_diff_portfolio_xts
    )
    
    dt_opts <- list(
        pageLength = 25,
        lengthMenu = c(10, 25, 50, 100),
        scrollX = TRUE,
        dom = "Bfrtip",
        buttons = c("copy", "csv", "excel"),
        deferRender = TRUE
    )
    
    invisible(lapply(names(tables), function(key) {
        id_tbl <- paste0("tbl_",  sanitize_id(key))
        id_plot1 <- paste0("plot1_", sanitize_id(key))
        id_plot2 <- paste0("plot2_", sanitize_id(key))
        id_plot3 <- paste0("plot3_", sanitize_id(key))
        id_metrics <- paste0("metrics_", sanitize_id(key))
        dt <- tables[[key]]
        xts_obj <- series_xts[[key]]
        
        # Plot
        output[[id_plot1]] <- render_pa_plot({
            req(xts_obj)
            PerformanceAnalytics::chart.CumReturns(
                xts_obj, wealth.index = TRUE, main = "Cumulative Return", ylab = "")})
        
        output[[id_plot2]] <- render_pa_plot({
            req(xts_obj)
            PerformanceAnalytics::chart.Drawdown(xts_obj, main = "Drawdowns", ylab = "")
        })
        
        output[[id_plot3]] <- render_pa_plot({
            req(xts_obj)
            PerformanceAnalytics::chart.RollingPerformance(
                xts_obj,
                width = 52, # 1-year on weekly data
                FUN = "SharpeRatio.annualized", scale = 52,  # choose your metric
                main = "Rolling Sharpe (52)"
            )
        })
        
        output[[id_metrics]] <- DT::renderDT({
            m <- performance_metrics_table(xts_obj, scale = 52)
            DT::datatable(
                m, rownames = FALSE, options = list(dom = "t", paging = FALSE)
            )
        }, server = FALSE)
        
        # Table (ensure newest first if a 'date' column exists)
        output[[id_tbl]] <- DT::renderDT({
            dt_show <- copy(dt)
            if ("date" %in% names(dt_show)) {
                # convert if needed and sort descending
                if (!inherits(dt_show$date, "Date") && !inherits(dt_show$date, "POSIXt")) {
                    suppressWarnings(dt_show[, date := as.Date(date)])
                }
                data.table::setorder(dt_show, -date)
            }
            
            # Optional: format 'date' column on the client side
            date_col_idx <- which(names(dt_show) == "date")
            col_defs <- if (length(date_col_idx) == 1) {
                list(list(targets = date_col_idx - 1L, render = JS(
                    "function(data,type,row,meta){",
                    "  if(type === 'display' || type === 'filter'){",
                    "    const d = new Date(data);",
                    "    if(!isNaN(d)) {",
                    "      const yyyy = d.getFullYear();",
                    "      const mm = String(d.getMonth()+1).padStart(2,'0');",
                    "      const dd = String(d.getDate()).padStart(2,'0');",
                    "      return `${yyyy}-${mm}-${dd}`;",
                    "    }",
                    "  }",
                    "  return data;",
                    "}"
                )))
            } else NULL
            
            DT::datatable(
                dt_show,
                rownames = FALSE,
                extensions = c("Buttons"),
                options = modifyList(dt_opts, list(columnDefs = col_defs))
            )
        }, server = TRUE)
    }))
}

shinyApp(ui = ui, server = server)
