% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/caseProcessMiningModule.R
\name{caseProcessMiningUI}
\alias{caseProcessMiningUI}
\alias{caseProcessMiningModule}
\title{Shiny module for process mining visualization and analysis of status development of registration}
\usage{
caseProcessMiningUI(id)

caseProcessMiningModule(
  input,
  output,
  session,
  .pm,
  audit_data = reactive(NULL),
  case_id = reactive(NULL),
  column = NULL,
  option_json = reactive(NULL)
)
}
\arguments{
\item{id}{Shiny input id}

\item{.pm}{pmClass for which the process mining is being done}

\item{audit_data}{dataset from audit functionality from shintoforms. If not provided using the standard event_data from a database}

\item{case_id}{case about we want to see event traces and process mining analyses}

\item{column}{the column which contains the states}

\item{option_json}{optional JSON so readable labels are plotted}
}
\description{
Shiny module for process mining visualization and analysis of status development of registration
}
