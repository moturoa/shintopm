#' Shiny module for process mining visualization and analysis of status development of registration
#' @param id Shiny input id
#' @export
#' @rdname processMiningModule

processMiningUI <- function(id){
  
  ns <- NS(id)
  
  softui::fluid_row(
    column(12,
           tags$p("Process Mining")
    )
  )
  
  
}

#' @export
#' @param .reg formClass for which the process mining is being done (call to formClass does not have event_data set as NULL)
#' @rdname processMiningModule
processMiningModule <- function(input, output, session, .reg){
  
  
}



