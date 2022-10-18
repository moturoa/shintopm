#' Shiny module for process mining visualization and analysis of status development of registration
#' @param id Shiny input id
#' @export
#' @rdname processMiningModule

processMiningUI <- function(id){

  ns <- NS(id)

  softui::fluid_row(
    column(12,
           plotOutput(ns("animated_process"))
    )
  )


}

#' @export
#' @param .reg formClass for which the process mining is being done (call to formClass does not have event_data set as NULL)
#' @rdname processMiningModule
processMiningModule <- function(input, output, session, .pm, audit_data = reactive(NULL), column = NULL, option_json = reactive(NULL)){

  event_log <- reactive({
    .pm$make_event_data(audit_data(), cid = "registration_id", aid = "new_val", aiid = "aiid",
                        tmst = "time_modified", lcid = "new_val", rid = "user_id")
  })

  output$animated_process <- renderPlot({
    req(event_log)
    event_log() %>% processanimateR::animate_process() %>% plot()

  })


}



