#' Shiny module for process mining visualization and analysis of status development of registration
#' @param id Shiny input id
#' @export
#' @rdname processMiningModule

processMiningUI <- function(id){

  ns <- NS(id)

  softui::fluid_page(
    softui::fluid_row(
      column(12,
             shiny::sliderInput(ns("slide_freq_pm"), label = "Trace frequency", min = 0, max = 1, step = 0.01, value = 1),
      )
    ),
    softui::fluid_row(
      column(6,
             tags$h3("Stappenkaart"),
             DiagrammeR::grVizOutput(ns("process_map_plot"))
      ),
      column(6,
             tags$h3("Overdracht-matrix"),
             plotOutput(ns("process_matrix_plot"))

      )
    ),
    tags$br(),
    softui::fluid_row(
      column(12,
             tags$h3("Tijdslijn"),
             processanimateR::processanimaterOutput(ns("animated_process"))
      )
    )

  )

}

#' @export
#' @param .reg formClass for which the process mining is being done (call to formClass does not have event_data set as NULL)
#' @rdname processMiningModule
processMiningModule <- function(input, output, session, .pm, audit_data = reactive(NULL), column = NULL, option_json = reactive(NULL)){

  event_log <- reactive({
    .pm$make_complete_log(audit_data(), column, option_json())
  })

  output$animated_process <- processanimateR::renderProcessanimater({
    req(event_log())

    graph <- processmapR::process_map(event_log(), render = F)
    model <- DiagrammeR::add_global_graph_attrs(graph, attr = "rankdir", value = "true", attr_type = "graph")
    event_log() %>% processanimateR::animate_process(processmap = model)

  })

  output$process_map_plot <- DiagrammeR::renderGrViz({

    event_log() %>% edeaR::filter_trace_frequency(percentage = input$slide_freq_pm) %>% processmapR::process_map(render = TRUE)

  })

  output$process_matrix_plot <- renderPlot({

    event_log() %>% edeaR::filter_trace_frequency(percentage = input$slide_freq_pm) %>% processmapR::precedence_matrix() %>% plot()

  })


}



