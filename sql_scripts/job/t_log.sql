-- Table: job.t_log

DROP TABLE IF EXISTS job.t_log;

CREATE TABLE IF NOT EXISTS job.t_log
(
    log_id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    log_date timestamp with time zone NOT NULL DEFAULT now(),
    job_id integer NOT NULL,
    error_level integer NOT NULL,
    log_message character varying(1024) COLLATE pg_catalog."default",
    CONSTRAINT pk_t_log PRIMARY KEY (log_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS job.t_log
    OWNER to postgres;