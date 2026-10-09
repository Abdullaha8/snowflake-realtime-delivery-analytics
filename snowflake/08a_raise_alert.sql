-- 08a_raise_alert.sql: the action every alert calls. Logs to OPS.ALERT_LOG and, when
-- SNOWFLAKE_ALERT_EMAIL is set (and 08b created the email integration), emails it.
-- An email failure is caught and logged, so it can never break the alert itself.

USE ROLE FOODDEL_TRANSFORM;
USE SCHEMA ${SNOWFLAKE_DATABASE}.OPS;

CREATE OR REPLACE PROCEDURE RAISE_ALERT(ALERT_NAME STRING, MESSAGE STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    recipient STRING DEFAULT '${SNOWFLAKE_ALERT_EMAIL}';
    emailed BOOLEAN DEFAULT FALSE;
    note STRING DEFAULT '';
BEGIN
    IF (recipient != '') THEN
        BEGIN
            CALL SYSTEM$SEND_EMAIL('FOODDEL_EMAIL', :recipient, '[fooddel] ' || :alert_name, :message);
            emailed := TRUE;
        EXCEPTION
            WHEN OTHER THEN
                note := ' (email failed: ' || SQLERRM || ')';
        END;
    END IF;
    INSERT INTO ALERT_LOG (ALERT_NAME, MESSAGE, EMAILED)
    VALUES (:alert_name, :message || :note, :emailed);
    RETURN alert_name || IFF(emailed, ' logged + emailed', ' logged') || note;
END;
$$;
