#' Shiny module for process mining visualization and analysis of status development of registration
#' @param id Shiny input id
#' @export
#' @rdname caseProcessMiningModule

caseProcessMiningUI <- function(id){

  ns <- NS(id)

  softui::fluid_row(
    column(12,
           uiOutput(ns("trace_case_id"))
    )
  )


}

#' @export
#' @param .pm pmClass for which the process mining is being done
#' @param audit_data dataset from audit functionality from shintoforms. If not provided using the standard event_data from a database
#' @param case_id case about we want to see event traces and process mining analyses
#' @param column the column which contains the states
#' @param option_json optional JSON so readable labels are plotted
#' @rdname caseProcessMiningModule
caseProcessMiningModule <- function(input, output, session, .pm, audit_data = reactive(NULL), case_id = reactive(NULL),
                                    column = NULL, option_json = reactive(NULL)){

  event_log <- reactive({
    .pm$get_eventlog_case(audit_data(), case_id(), column, option_json())
  })

  output$trace_case_id <- renderUI({

    event_log <- event_log()

    req(event_log)

    softui::fluid_row(
      column(12,
             softui::box(title = glue("Proces van {case_id()}"), width = 12,
                         softui::fluid_row(
                           column(6,
                                  tags$h3("Stappenspoor"),
                                  plotOutput(session$ns("trace_explorer_plot"))
                           ),
                           column(6,
                                  tags$h3("Stappenkaart"),
                                  DiagrammeR::grVizOutput(session$ns("process_map_plot"))

                           )
                         ),
                         softui::fluid_row(
                           column(6,
                                  tags$h3("Actoren"),
                                  DiagrammeR::grVizOutput(session$ns("resource_map_plot"))

                           ),
                           column(6,
                                  tags$h3("Overdracht-matrix"),
                                  plotOutput(session$ns("process_matrix_plot"))
                           )
                         )
             )
      )
    )

  })

  output$trace_explorer_plot <- renderPlot({

    event_log() %>% processmapR::trace_explorer() %>% plot()

  })

  output$process_map_plot <- DiagrammeR::renderGrViz({

    event_log() %>% processmapR::process_map(render = TRUE)

  })


  output$process_matrix_plot <- renderPlot({

    event_log() %>% processmapR::precedence_matrix() %>% plot()

  })

  output$resource_map_plot <- DiagrammeR::renderGrViz({

    event_log() %>% processmapR::resource_map()

  })


}
