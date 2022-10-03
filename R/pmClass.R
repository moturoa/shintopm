#' R6 Class voor Process Mining applicaties
#' @importFrom R6 R6Class
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
    #' @param form_data If the user uses the shintoforms package this table is used to get the current state
    #' @param form_audit If the user uses the shintoforms package this table is used to get the history
    #' @param event_data If the user want to use their own event data
    #' @param event_columns If the user uses own event_data, the columns can be specified here.
    initialize = function(config_file = "conf/config.yml",
                          what,
                          schema = NULL,
                          pool = TRUE,
                          sqlite = NULL,
                          form_data = NULL,
                          form_audit = NULL,
                          event_data = NULL,
                          event_columns = list(
                            case = "case_id",
                            activity = "activity_id",
                            activity_instance = "activity_id_instance",
                            eventtime = "eventtime",
                            lifecycle = "lifecycle_id",
                            resource = "resource")
    ){

      self$connect_to_database(config_file, schema, what, pool, sqlite)


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
          self$con <- dbPool(RSQLite::SQLite(), dbname = sqlite)
        } else {
          self$con <- dbConnect(RSQLite::SQLite(), dbname = sqlite)
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

          dbDisconnect(self$con)
        }

      } else {
        flog.info("Not closing an invalid or null connection", name = "DBR6")
      }
    },


    make_column = function(table, column, type = "varchar"){

      if(is.null(self$schema)){
        qu <- glue::glue("alter table {table} add column {column} {type}")
      } else {
        qu <- glue::glue("alter table {self$schema}.{table} add column {column} {type}")
      }

      dbExecute(self$con, qu)

    },


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

    append_data = function(table, data){

      #flog.info(glue("dbWriteTable({table})"), append = TRUE, name = "DBR6")

      if(!is.null(self$schema)){

        try(
          dbWriteTable(self$con,
                       name = DBI::Id(schema = self$schema, table = table),
                       value = data,
                       append = TRUE)
        )

      } else {

        try(
          dbWriteTable(self$con,
                       name = table,
                       value = data,
                       append = TRUE)
        )

      }


    },

    table_columns = function(table){

      if(is.null(self$schema)){
        names(self$query(glue("select * from {table} where false")))
      } else {
        names(self$query(glue("select * from {self$schema}.{table} where false")))
      }


    },

    query = function(txt, glue = TRUE, quiet = FALSE){

      if(glue)txt <- glue::glue(txt)
      # if(!quiet){
      #   flog.info(glue("query({txt})"), name = "DBR6")
      # }
      #

      try(
        dbGetQuery(self$con, txt)
      )

    },

    has_value = function(table, column, value){

      if(!is.null(self$schema)){
        out <- self$query(glue("select {column} from {self$schema}.{table} where {column} = '{value}' limit 1"))
      } else {
        out <- self$query(glue("select {column} from {table} where {column} = '{value}' limit 1"))
      }

      nrow(out) > 0
    },


    # set verwijderd=1 where naam=gekozennaam.
    # replace_value_where("table", 'verwijderd', 'true', 'naam', 'gekozennaam')
    replace_value_where = function(table, col_replace, val_replace, col_compare, val_compare,
                                   query_only = FALSE, quiet = FALSE){


      if(!is.null(self$schema)){
        if(is.logical(val_replace) & !is.na(val_replace)){
          query <- glue("update {self$schema}.{table} set {col_replace} = ?val_replace::boolean where ",
                        "{col_compare} = ?val_compare") %>% as.character()
        } else {
          query <- glue("update {self$schema}.{table} set {col_replace} = ?val_replace where ",
                        "{col_compare} = ?val_compare") %>% as.character()
        }

      } else {
        if(is.logical(val_replace) & !is.na(val_replace)){
          query <- glue("update {table} set {col_replace} = ?val_replace::boolean where ",
                        "{col_compare} = ?val_compare") %>% as.character()
        } else {
          query <- glue("update {table} set {col_replace} = ?val_replace where ",
                        "{col_compare} = ?val_compare") %>% as.character()
        }

      }

      query <- sqlInterpolate(DBI::ANSI(),
                              query,
                              val_replace = val_replace, val_compare = val_compare)

      if(query_only)return(query)

      # if(!quiet){
      #   flog.info(query, name = "DBR6")
      # }

      dbExecute(self$con, query)

    },


    #' @description Make choices (for selectInput) based on values and names
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

    from_json = function(x, ...){

      shintocatman::from_json(x, ...)

    },

    to_json = function(x, ...){

      shintocatman::to_json(x, ...)

    },



    ####### Process Mining #####

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

    delete_events_from_case = function(case_id){
      if(!is.null(self$schema)){
        qu <- glue::glue("DELETE FROM {self$schema}.{self$event_data} WHERE {self$event_columns$case} = '{case_id}'")
      } else {
        qu <- glue::glue("DELETE FROM {self$event_data} WHERE {self$event_columns$case} = '{case_id}'")
      }

      dbExecute(self$con, qu)
    },


    make_event_data = function(data){
      bupaR::eventlog(data,
                      case_id = self$event_columns$case,
                      activity_id = self$event_columns$activity,
                      activity_instance_id = self$event_columns$activity_instance,
                      timestamp = self$event_columns$eventtime,
                      lifecycle_id = self$event_columns$lifecycle,
                      resource_id = self$event_columns$resource)
    },

    get_eventdata_case = function(id, column, option_json){



      data <- self$read_table(self$event_data, lazy = TRUE) %>%
        filter(!!sym(self$event_columns$case) == !!id) %>%
        collect

      event_data <- self$make_event_data(data)


      if(is.null(option_json)){

        event_data <- event_data %>%
          replace_na(list(activity_id = "Niet ingevuld"))

      } else {
        phases <- unlist(option_json)
        phases <- data.frame(number = names(phases), phase = phases)

        firstname <- self$event_columns$activity
        join_cols = c("number")
        names(join_cols) <- firstname

        event_data <- left_join(event_data, phases, by = join_cols)

        event_data <- event_data %>%
          mutate(activity_id = phase) %>%
          select(-c("phase")) %>%
          replace_na(list(activity_id = "Niet ingevuld"))
      }


    },

    get_auditdata_case = function(id, column, option_json){

    }

  )

)
