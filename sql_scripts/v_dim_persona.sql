-- View: dm.v_dim_persona

-- DROP VIEW dm.v_dim_persona;

CREATE OR REPLACE VIEW dm.v_dim_persona
AS

SELECT A.persona_id
	, A.persona_name
	, A.description
	FROM dwh.t_persona A
	WHERE A.dwh_status = 'A'
;

ALTER TABLE dm.v_dim_persona
    OWNER TO postgres;