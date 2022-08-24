#' load_data_Redshift_AZ
#'
#' Loads data from an AWS Reshift instance.
#'
#' When the index date is the date of consent, only the patient reported data is used.
#'
#' @param conn A connection to a redshift instance. Tables are assumed to be stored under a schema called "ibd_plexus"
#' @param cohort The cohort to load. Either RISK, QORUS, or SPARC.
#' @param domains The domains to load. Default is "All". Must be a character string.
#' @param data_type The data source to load either case report forms, electronic medical record or both. Options are both, crf or emr.
#'
#' @return A list of dataframes for each domain. If both sources are loaded, emr and crf data are combined.
#' @export
load_data_redshift <- function(conn, cohort = c("RISK", "QORUS", "SPARC"), domains = c("ALL"), data_type = c("BOTH", "CRF", "EMR"))
{
  if( any(c("RISK", "QORUS") %in% toupper(cohort) ))
  {
    stop("Only SPARC data is currently supported. Please use one of the other load_data() implemetations.")
  }

  data <- list()

#  data[["procedures"]] <- load_procedures(con)
  demographics_tables <- grep(pattern = "_demographics",
                           x = dbListTables(conn =con,schema = "ibd_plexus"),
                           value = TRUE)
  demographics <- lapply(demographics_tables,function(x){
    tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
    bind_rows()
  names(demographics) <- toupper(names(demographics))

  demographics <- demographics %>%
    mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))

  data[["demographics"]] <- demographics
#########
  encounter_tables <- grep(pattern = "_encounter_",
                           x = dbListTables(conn =con,schema = "ibd_plexus"),
                           value = TRUE)
  encounter <- lapply(encounter_tables,function(x){
    tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
    bind_rows()
  names(encounter) <- toupper(names(encounter))
  encounter <- encounter  %>% rename(VISIT_ENCOUNTER_ID = VISITENC_ID) %>%
    mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))
  data[["encounter"]] <- encounter
#########
  diagnosis_tables <- grep(pattern = "_diagnosis_",
                           x = dbListTables(conn =con,schema = "ibd_plexus"),
                           value = TRUE)
  diagnosis <- lapply(diagnosis_tables,function(x){
    tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
    bind_rows()
  names(diagnosis) <- toupper(names(diagnosis))
  diagnosis <- diagnosis %>%
    mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))
  data[["diagnosis"]] <- diagnosis

#########
  if( any(c("OBSERVATIONS","ALL") %in% toupper(domains) )){

    observations_tables <- grep(pattern = "_observations_",
                             x = dbListTables(conn =con,schema = "ibd_plexus"),
                             value = TRUE)
    observations <- lapply(observations_tables,function(x){
      tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
      bind_rows()
    names(observations) <- toupper(names(observations))
    data[["observations"]]  <- observations  %>%
      mutate(OBS_TEST_CONCEPT_NAME = ifelse(OBS_TEST_CONCEPT_NAME == "Constitutional- General Well-Being", "Constitutional - General Well-Being", OBS_TEST_CONCEPT_NAME)) %>%
      mutate(across(everything(), ~ replace(., . %in% c("N.A.", "NA", "N/A", "", " "), NA))) %>%
      mutate(OBS_TEST_RESULT_DATE = dmy(OBS_TEST_RESULT_DATE)) %>%
      mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))
    }
  #########
  meds_tables <- grep(pattern = "_prescriptions_",
                           x = dbListTables(conn =con,schema = "ibd_plexus"),
                           value = TRUE)
  prescriptions <- lapply(meds_tables,function(x){
    tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
    bind_rows()
  names(prescriptions) <- toupper(names(prescriptions))
  data[["prescriptions"]] <- prescriptions %>%
    mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))

  #########
  procedure_tables <- grep(pattern = "_procedures_",
                      x = dbListTables(conn =con,schema = "ibd_plexus"),
                      value = TRUE)
  procedures <- lapply(procedure_tables,function(x){
    tbl(con,DBI::Id(schema = "ibd_plexus", table = x)) %>% collect() } ) %>%
    bind_rows()
 names(procedures) <- toupper(names(procedures))
  procedures <- procedures %>% rename("SES-CD_Subscore" = "SES_CD_SUBSCORE") # rename the column for consistency with CCF names.
  data[["procedures"]] <- procedures %>%
    mutate(DEIDENTIFIED_MASTER_PATIENT_ID  = as.character(DEIDENTIFIED_MASTER_PATIENT_ID ))

  return( data )
}
