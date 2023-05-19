#' R6 Class voor Process Mining applicaties
#' @importFrom R6 R6Class
#' @importFrom DBI dbIsValid
#' @param lock_objects Boolean to say if objects are locked
#' @param public List of public functions in R6 class
#' @export
pmClass <- R6::R6Class(
  lock_objects = FALSE,

  public = list(

    #' @description Make new process mining application object.
    #' @param config_file Path to DB config
    #' @param what Entry in config for DB connection
    #' @param schema DB schema
    #' @param pool If TRUE, connects with dbPool
    #' @param sqlite If path to an SQLite file, uses SQLite.
    #' @param db_connection A valid [DBI::dbConnection()] object; for postgres connections only.
    #' @param form_data If the user uses the shintoforms package this table is used to get the current state
    #' @param form_data_id Name of column that specifies the ids for cases/registrations in the form_data table
    #' @param form_audit If the user uses the shintoforms package this table is used to get the history
    #' @param form_audit_id Name of column that specifies the ids for cases/registrations in the audit_data table
    #' @param event_data If the user want to use their own event data
    #' @param event_columns If the user uses own event_data, the columns can be specified here.
    initialize = function(config_file = "conf/config.yml",
                          what,
                          schema = NULL,
                          pool = TRUE,
                          sqlite = NULL,
                          db_connection = NULL,
                          form_data = NULL,
                          form_data_id = NULL,
                          form_audit = NULL, # NIET OVERBODIG; Iets maken dat je ook algemeen vanuit hier kan maken zonder preprocessed form_audits mee te geven.
                          form_audit_id = NULL,
                          event_data = NULL,
                          event_columns = list(
                            case = "case_id",
                            activity = "activity_id",
                            activity_instance = "activity_id_instance",
                            eventtime = "eventtime",
                            lifecycle = "lifecycle_id",
                            resource = "resource")
    ){

      if(is.null(db_connection)){
        self$connect_to_database(config_file, schema, what, pool, sqlite)
      } else {
        if(!DBI::dbIsValid(db_connection)){
          stop("Please pass a valid dbConnection object")
        }

        self$con <- db_connection
        self$schema <- schema
        self$pool <- pool  #unused with passed db_connection?
        self$dbtype <- "postgres"

      }


      self$form_data <- form_data
      self$form_data_id <- form_data_id

      self$form_audit_id <- form_audit_id


      self$event_data <- event_data
      self$event_columns <- event_columns

      if(!is.null(self$event_data)){
        eventcols <- self$table_columns(self$event_data)
        given_cols <- unlist(self$event_columns) %in% eventcols

        if(any(!given_cols)){
          stop(paste("Columns in event_columns not found:", paste(eventcols[!given_cols], collapse=",")))
        }
      }

    },

    #----- Generic database methods

    #' @description Connect to a database
    #' @param config_file Path to DB config
    #' @param what Entry in config for DB connection
    #' @param schema DB schema
    #' @param pool If TRUE, connects with dbPool
    #' @param sqlite If path to an SQLite file, uses SQLite.
    connect_to_database = function(config_file = NULL,
                                   schema = NULL,
                                   what = NULL,
                                   pool = TRUE,
                                   sqlite = NULL){

      if(!is.null(sqlite)){

        if(!file.exists(sqlite)){
          stop("SQlite not found, check path")
        }

        self$schema <- NULL
        self$dbname <- sqlite
        self$pool <- pool

        if(pool){
          self$con <- pool::dbPool(RSQLite::SQLite(), dbname = sqlite)
        } else {
          self$con <- DBI::dbConnect(RSQLite::SQLite(), dbname = sqlite)
        }

        self$dbtype <- "sqlite"

      } else {

        self$schema <- schema
        self$dbname <- what
        self$pool <- pool

        cf <- config::get(what, file = config_file)

        print("----CONNECTING TO----")
        print(cf$dbhost)

        self$dbuser <- cf$dbuser

        if(pool){
          flog.info("pool::dbPool", name = "DBR6")
          response <- try(pool::dbPool(RPostgres::Postgres(),
                                       dbname = cf$dbname,
                                       host = cf$dbhost,
                                       port = cf$dbport,
                                       user = cf$dbuser,
                                       password = cf$dbpassword,
                                       minSize = 1,
                                       maxSize = 25,
                                       idleTimeout = 60*60*1000))
        } else {
          flog.info("DBI::dbConnect", name = "DBR6")
          response <- try(DBI::dbConnect(RPostgres::Postgres(),
                                         dbname = cf$dbname,
                                         host = cf$dbhost,
                                         port = cf$dbport,
                                         user = cf$dbuser,
                                         password = cf$dbpassword))
        }

        if(!inherits(response, "try-error")){
          self$con <- response
        }


        self$dbtype <- "postgres"

      }


    },

    #' @description Close database connection
    close = function(){

      if(!is.null(self$con) && dbIsValid(self$con)){

        if(self$pool){
          flog.info("poolClose", name = "DBR6")

          poolClose(self$con)
        } else {
          flog.info("dbDisconnect", name = "DBR6")

          DBI::dbDisconnect(self$con)
        }

      } else {
        flog.info("Not closing an invalid or null connection", name = "DBR6")
      }
    },

    #' @description Add a column to a table
    #' @param table The table to which the column is added
    #' @param column the name of the column to be added
    #' @param type the type of the column to be added
    make_column = function(table, column, type = "varchar"){

      if(is.null(self$schema)){
        qu <- glue::glue("alter table {table} add column {column} {type}")
      } else {
        qu <- glue::glue("alter table {self$schema}.{table} add column {column} {type}")
      }

      DBI::dbExecute(self$con, qu)

    },

    #' @description Function to read a complete table from a database
    #' @param table The table which must be retrieved
    #' @param lazy Boolean whether the entire table must retrieved or whether is should be done lazily
    read_table = function(table, lazy = FALSE){

      #tictoc::tic(glue("tbl({table})"))

      if(!is.null(self$schema)){
        out <- tbl(self$con, in_schema(self$schema, table))
      } else {
        out <- tbl(self$con, table)
      }


      if(!lazy){
        out <- collect(out)
      }

      #tictoc::toc()

      out

    },

    #' @description Function to add data to a table
    #' @param table The table to which the data must be added
    #' @param data data to be added to the data in the table
    append_data = function(table, data){

      #flog.info(glue("dbWriteTable({table})"), append = TRUE, name = "DBR6")

      if(!is.null(self$schema)){

        try(
          DBI::dbWriteTable(self$con,
                            name = DBI::Id(schema = self$schema, table = table),
                            value = data,
                            append = TRUE)
        )

      } else {

        try(
          DBI::dbWriteTable(self$con,
                            name = table,
                            value = data,
                            append = TRUE)
        )

      }


    },

    #' @description Get the column names from a table
    #' @param table The table from which the names must be retrieved
    table_columns = function(table){

      if(is.null(self$schema)){
        names(self$query(glue::glue("select * from {table} where false")))
      } else {
        names(self$query(glue::glue("select * from {self$schema}.{table} where false")))
      }


    },

    #' @description Function to execute a query
    #' @param txt The text containing the query
    #' @param glue Boolean to indicate whether the text contains glue statements
    query = function(txt, glue = TRUE){

      if(glue)txt <- glue::glue(txt)

      try(
        DBI::dbGetQuery(self$con, txt)
      )

    },

    #' @description Checks whether a value occurs in a column
    #' @param table The table which should be checked
    #' @param column The column that should be checked
    #' @param value The value which should be checked for
    has_value = function(table, column, value){

      if(!is.null(self$schema)){
        out <- self$query(glue::glue("select {column} from {self$schema}.{table} where {column} = '{value}' limit 1"))
      } else {
        out <- self$query(glue::glue("select {column} from {table} where {column} = '{value}' limit 1"))
      }

      nrow(out) > 0
    },


    # set verwijderd=1 where naam=gekozennaam.
    # replace_value_where("table", 'verwijderd', 'true', 'naam', 'gekozennaam')
    #' @description Function to dynamically replace values
    #' @param table The table in which values should be replaced
    #' @param col_replace The column that should be replaced
    #' @param val_replace The value that should be replaced
    #' @param col_compare The column to compare to
    #' @param val_compare The value to compare to
    #' @param query_only Boolean indicating if the query should be returned. FALSE means it is also executed
    replace_value_where = function(table, col_replace, val_replace, col_compare, val_compare,
                                   query_only = FALSE){


      if(!is.null(self$schema)){
        if(is.logical(val_replace) & !is.na(val_replace)){
          query <- glue::glue("update {self$schema}.{table} set {col_replace} = ?val_replace::boolean where ",
                              "{col_compare} = ?val_compare") %>% as.character()
        } else {
          query <- glue::glue("update {self$schema}.{table} set {col_replace} = ?val_replace where ",
                              "{col_compare} = ?val_compare") %>% as.character()
        }

      } else {
        if(is.logical(val_replace) & !is.na(val_replace)){
          query <- glue::glue("update {table} set {col_replace} = ?val_replace::boolean where ",
                              "{col_compare} = ?val_compare") %>% as.character()
        } else {
          query <- glue::glue("update {table} set {col_replace} = ?val_replace where ",
                              "{col_compare} = ?val_compare") %>% as.character()
        }

      }

      query <- DBI::sqlInterpolate(DBI::ANSI(),
                                   query,
                                   val_replace = val_replace, val_compare = val_compare)

      if(query_only)return(query)

      DBI::dbExecute(self$con, query)

    },


    #' @description Make choices (for selectInput) based on values and names
    #' @param values_from Column to specify where the values must come from
    #' @param names_from Column to specify where the names must come from
    #' @param data The dataset from which the values and names come
    #' @param sort Boolean indicating if the options must be sorted
    make_choices = function(values_from, names_from = values_from, data = NULL, sort = TRUE){

      data <- data %>%
        distinct(!!sym(values_from), !!sym(names_from))

      out <- data[[values_from]] %>%
        setNames(data[[names_from]])

      # Sorteer op labels, niet op waardes
      if(sort){
        out <- out[order(names(out))]
      }

      return(out)

    },

    #' @description Unpack a JSON field to make a named vector
    #' @param x the json field
    choices_from_json = function(x){

      val <- self$from_json(x)
      nms <- unlist(unname(val))

      out <- setNames(names(val), nms)

      out2 <- suppressWarnings({
        setNames(as.integer(out), nms)
      })

      if(!any(is.na(out2))){
        out <- out2
      }

      out
    },

    #' @description Unpack a JSON field to make a named vector
    #' @param x the json field
    #' @param ... other arguments
    from_json = function(x, ...){

      shintocatman::from_json(x, ...)

    },

    #' @description Form a named vector into a JSON field
    #' @param x the named vector
    #' @param ... other arguments
    to_json = function(x, ...){

      shintocatman::to_json(x, ...)

    },



    ####### Process Mining #####

    #' @description Add an event to the event_data
    #' @param case_id ID for the case
    #' @param activity Activity of the event
    #' @param resource Resource of event, who performed the activity
    #' @param act_ins Optional instance of the activity
    #' @param e_time Time of the event
    #' @param lc Lifecycle phase
    add_event = function(case_id, activity, resource, act_ins = NULL, e_time = NULL, lc = NULL){

      if(is.null(act_ins)){
        activity_instance <- uuid::UUIDgenerate()
      } else {
        activity_instance <- act_ins
      }

      if(is.null(e_time)){
        eventtime <- format(Sys.time())
      } else {
        eventtime <- e_time
      }

      if(is.null(lc)){
        lifecycle <- "Uitvoering"
      } else {
        lifecycle <- lc
      }


      new_event <- data.frame(
        case = case_id,
        activity = activity,
        activity_instance = activity_instance,
        eventtime = eventtime,
        lifecycle = lifecycle,
        resource = resource
      )

      new_event <- dplyr::rename_with(new_event,
                                      .fn = function(x){
                                        unname(unlist(self$event_columns[x]))
                                      })

      res <- self$append_data(self$event_data, new_event)

      # TRUE if success (append_data has a try())
      return(!inherits(res, "try-error"))

    },

    #' @description Delete an event from the event_data
    #' @param case_id ID for the case
    delete_events_from_case = function(case_id){
      if(!is.null(self$schema)){
        qu <- glue::glue("DELETE FROM {self$schema}.{self$event_data} WHERE {self$event_columns$case} = '{case_id}'")
      } else {
        qu <- glue::glue("DELETE FROM {self$event_data} WHERE {self$event_columns$case} = '{case_id}'")
      }

      DBI::dbExecute(self$con, qu)
    },


    #' @description Make a bupaR eventlog from a dataset
    #' @param data The dataset that should be transformed to an eventlog
    #' @param cid Column that specifies the ID for the case
    #' @param aid Column that specifies the activity of the event
    #' @param aiid Column that specifies the activity instance of the event
    #' @param tmst Column that specifes the timestamp of the event
    #' @param lcid Column that specifies the lifecycle id of the event
    #' @param rid Column that specifies the resource of the event
    make_event_data = function(data, cid = self$event_columns$case, aid = self$event_columns$activity, aiid = self$event_columns$activity_instance,
                               tmst = self$event_columns$eventtime, lcid = self$event_columns$lifecycle, rid = self$event_columns$resource){
      bupaR::eventlog(data,
                      case_id = cid,
                      activity_id = aid,
                      activity_instance_id = aiid,
                      timestamp = tmst,
                      lifecycle_id = lcid,
                      resource_id = rid)
    },

    #' @description Get the eventlog of a specific case
    #' @param audit_data If specified, use this data as audit data, if not specified, use event_data
    #' @param case_id Case from which the eventlog must be obtained
    #' @param column Obligated field that specifies which column should be mined
    #' @param option_json If the field has json names, this is the json to transform these numbers into names
    get_eventlog_case = function(audit_data, case_id, column, option_json){

      # TODO: CAN WE USE NEW_VAL? OR SHOULD IT BE GENERIC? --> type, time, etc. allemaal algemene kolommen maken

      if(is.null(column)){
        stop("A column MUST be defined in order to execute process mining")
      }

      if(!is.null(audit_data)){
        event_data <- audit_data %>%
          dplyr::filter(registration_id == !!case_id) %>%
          dplyr::filter(variable == !!column | type == "Aanmaak")

        event_data$aiid <- replicate(nrow(event_data), uuid::UUIDgenerate())

        # The creation event is not filled in yet. For this we have to fill in the table so a complete eventlog can be created.
        # There are four cases;
        # - a registration has been made and the aspect which is being mined is not filled in
        # - a registration has been made and the aspect which is being mined was not filled in initially but has been now
        # - a registration has been made and the aspect which is being mined has been filled in but has not changed
        # - a registration has been made and the aspect which is being mined has been filled in and has been changed
        # The first en second case is being handled by the NA-replaces down the line.
        # The third case is a case where only the creation row is present; the creation row does not have any filled in variables.
        # Hence, in this case we should fetch this from the self$formdata


        if(nrow(event_data) == 1 && event_data$type == "Aanmaak"){
          current_reg_act <- self$read_table(self$form_data, lazy = TRUE) %>%
            dplyr::filter(!!rlang::sym(self$form_data_id) == case_id) %>%
            dplyr::collect() %>%
            dplyr::select(!!rlang::sym(column)) %>%
            dplyr::pull(!!rlang::sym(column))

          if(!is.na(current_reg_act)){
            if(current_reg_act == ""){
              current_reg_act <- NA
            }
          }

          event_data$new_val <- current_reg_act
        } else if(nrow(event_data) > 1){
          # If this is the case the fourth case applies. We should get the  old value' from the first U event and put it in the 'new value' of the C event.

          # TODO --> Dit is nu functie, aanpassen? get_first_value_before_edits
          u_events <- event_data %>%
            dplyr::filter(type == "Wijziging") %>%
            dplyr::arrange(time_modified)

          first_value <- u_events$old_val[1]

          if(!is.na(first_value) && first_value == ""){
            first_value <- NA
          }

          event_data[event_data$type == "Aanmaak",]$new_val <- first_value

        }

        event_data <- self$make_event_data(event_data, cid = "registration_id", aid = "new_val", aiid = "aiid",
                                           tmst = "time_modified", lcid = "new_val", rid = "user_id")

        activity_column <- "new_val"

      } else if(!is.null(self$event_data)) {
        data <- self$read_table(self$event_data, lazy = TRUE) %>%
          dplyr::filter(!!rlang::sym(self$event_columns$case) == !!case_id) %>%
          dplyr::collect

        event_data <- self$make_event_data(data)

        activity_column <- self$event_columns$activity

      } else {
        stop("No audit data has been provided in the function call while no event_data table has been defined")
      }

      if(is.null(option_json)){

        event_data <- event_data %>%
          dplyr::mutate(!!activity_column := tidyr::replace_na(!!rlang::sym(activity_column), "Niet ingevuld"))

      } else {
        phases <- unlist(option_json)
        phases <- data.frame(number = names(phases), phase = phases)

        firstname <- activity_column
        join_cols = c("number")
        names(join_cols) <- firstname

        event_data <- left_join(event_data, phases, by = join_cols)

        event_data <- event_data %>%
          dplyr::mutate(!!rlang::sym(activity_column) := phase) %>%
          dplyr::select(-c("phase")) %>%
          dplyr::mutate(!!activity_column := tidyr::replace_na(!!rlang::sym(activity_column), "Niet ingevuld"))
      }


    },


    #' @description Get the eventlog of all cases
    #' @param audit_data If specified, use this data as audit data, if not specified, use event_data
    #' @param column Obligated field that specifies which column should be mined
    #' @param option_json If the field has json names, this is the json to transform these numbers into names
    make_complete_log = function(audit_data, column, option_json){

      # TODO: CAN WE USE NEW_VAL? OR SHOULD IT BE GENERIC?

      if(is.null(column)){
        stop("A column MUST be defined in order to execute process mining")
      }

      if(!is.null(audit_data)){
        event_data <- audit_data %>%
          dplyr::filter(variable == !!column | type == "Aanmaak")

        # All registrations that only occur once have only current data, which must be retrieved from the registration table
        one_occurrence <- event_data %>%
          dplyr::count(!!rlang::sym(self$form_audit_id)) %>%
          dplyr::filter(n == 1)

        one_occ_reg <- self$read_table(self$form_data) %>%
          dplyr::filter(!!rlang::sym(self$form_data_id) %in% one_occurrence[[self$form_data_id]]) %>%
          dplyr::select(!!rlang::sym(self$form_data_id), !!rlang::sym(column))

        event_data <- event_data %>%
          dplyr::left_join(one_occ_reg, by = setNames(self$form_audit_id, self$form_data_id)) %>%
          dplyr::mutate(new_val = coalesce(new_val, !!rlang::sym(column))) %>%
          dplyr::select(-!!rlang::sym(column)) %>%
          dplyr::mutate(new_val = na_if(new_val, ""))

        # All registrations that have multiple occurrences should get the old_val from the first U-event as value in the new_val in the C-event
        mult_occ_reg <- event_data %>%
          dplyr::filter(!(!!rlang::sym(self$form_audit_id) %in% one_occurrence[[self$form_data_id]]))

        sapply(unique(mult_occ_reg$registration_id), function(x){

          dt <- mult_occ_reg %>%
            dplyr::filter(!!rlang::sym(self$form_audit_id) == x)

          res <- self$get_first_value_before_edits(dt)

          event_data[event_data$type == "Aanmaak" & event_data[[self$form_audit_id]] == x,]$new_val <<- res


        })

        event_data$aiid <- replicate(nrow(event_data), uuid::UUIDgenerate())

        event_data <- self$make_event_data(event_data, cid = "registration_id", aid = "new_val", aiid = "aiid",
                                           tmst = "time_modified", lcid = "new_val", rid = "user_id")

        activity_column <- "new_val"


      } else if(!is.null(self$event_data)) {


      } else {
        stop("No audit data has been provided in the function call while no event_data table has been defined")
      }

      # TODO: Hier zitten wat dubbele stukken code in tov get_eventlog_case(), kijk of je dit kan verbeteren

      if(is.null(option_json)){

        event_data <- event_data %>%
          dplyr::mutate(!!activity_column := tidyr::replace_na(!!rlang::sym(activity_column), "Niet ingevuld"))

      } else {
        phases <- unlist(option_json)
        phases <- data.frame(number = names(phases), phase = phases)

        firstname <- activity_column
        join_cols = c("number")
        names(join_cols) <- firstname

        event_data <- left_join(event_data, phases, by = join_cols)

        event_data <- event_data %>%
          dplyr::mutate(!!rlang::sym(activity_column) := phase) %>%
          dplyr::select(-c("phase")) %>%
          dplyr::mutate(!!activity_column := tidyr::replace_na(!!rlang::sym(activity_column), "Niet ingevuld"))
      }



    },

    #' @description For an audit file get the option that was filled in when the case was created
    #' @param data Audit datafile for one case
    get_first_value_before_edits = function(data){

      u_events <- data %>%
        dplyr::filter(type == "Wijziging") %>%
        dplyr::arrange(time_modified)

      first_value <- u_events$old_val[1]

      if(!is.na(first_value) && first_value == ""){
        first_value <- NA
      }

      return(first_value)

    }


  )

)
