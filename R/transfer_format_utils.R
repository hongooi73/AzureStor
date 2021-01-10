#' @export
storage_save_rds <- function(object, container, file, ...)
{
    conn <- rawConnection(serialize(object, NULL, ...), open="rb")
    storage_upload(container, conn, file)
}


#' @export
storage_load_rds <- function(container, file, ...)
{
    conn <- storage_download(container, file, NULL)
    unserialize(conn, ...)
}


#' @export
storage_save_rdata <- function(..., container, file, envir=parent.frame())
{
    # save to a temporary file as saving to a connection disables compression
    tmpsave <- tempfile(fileext=".rdata")
    on.exit(unlink(tmpsave))
    save(..., file=tmpsave, envir=envir)
    storage_upload(container, tmpsave, file)
}


#' @export
storage_load_rdata <- function(container, file, envir=parent.frame(), ...)
{
    conn <- storage_download(container, file, NULL)
    load(rawConnection(conn, open="rb"), envir=envir, ...)
}


#' @export
storage_write_delim <- function(object, container, file, delim="\t ", ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_delim_readr
    else storage_write_delim_base
    func(object, container, file, delim=delim, ...)
}


storage_write_delim_readr <- function(object, container, file, delim="\t ", ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    readr::write_delim(object, conn, delim=delim, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


storage_write_delim_base <- function(object, container, file, delim="\t", ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    write.table(object, conn, sep=delim, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


#' @export
storage_write_csv <- function(object, container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_csv_readr
    else storage_write_csv_base
    func(object, container, file, delim=delim, ...)
}


storage_write_csv_readr <- function(object, container, file, ...)
{
    storage_write_delim_readr(object, container, file, delim=",", ...)
}


storage_write_csv_base <- function(object, container, file, ...)
{
    storage_write_delim_base(object, container, file, delim=",", ...)
}


#' @export
storage_write_csv2 <- function(object, container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_write_csv2_readr
    else storage_write_csv2_base
    func(object, container, file, ...)
}


storage_write_csv2_readr <- function(object, container, file, ...)
{
    conn <- rawConnection(raw(0), open="r+b")
    readr::write_csv2(object, conn, ...)
    seek(conn, 0)
    storage_upload(container, conn, file)
}


storage_write_csv2_base <- function(object, container, file, ...)
{
    storage_write_delim_base(object, container, file, delim=";", dec=",", ...)
}


#' @export
storage_read_delim <- function(container, file, delim="\t", ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_delim_readr
    else storage_read_delim_base
    func(container, file, delim=delim, ...)
}


storage_read_delim_readr <- function(container, file, delim="\t", ...)
{
    txt <- storage_download(container, file, NULL)
    readr::read_delim(txt, delim=delim, ...)
}


storage_read_delim_base <- function(container, file, delim="\t", ...)
{
    txt <- storage_download(container, file, NULL)
    read.delim(text=rawToChar(txt), sep=delim, ...)
}


#' @export
storage_read_csv <- function(container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_csv_readr
    else storage_read_csv_base
    func(container, file, ...)
}


storage_read_csv_readr <- function(container, file, ...)
{
    storage_read_delim_readr(container, file, delim=",", ...)
}


storage_read_csv_base <- function(container, file, ...)
{
    storage_read_delim_base(container, file, delim=",", ...)
}


#' @export
storage_read_csv2 <- function(container, file, ...)
{
    func <- if(requireNamespace("readr"))
        storage_read_csv2_readr
    else storage_read_csv2_base
    func(container, file, ...)
}


storage_read_csv2_readr <- function(container, file, ...)
{
    txt <- storage_download(container, file, NULL)
    readr::read_csv2(txt, ...)
}


storage_read_csv2_base <- function(container, file, ...)
{
    storage_read_delim_base(container, file, delim=";", dec=",", ...)
}

