CREATE OR REPLACE FUNCTION LPwithdraw(tb text) RETURNS void
AS $T1$
DECLARE
    rec record;
BEGIN
--  delete the extra 3 columns of the target table
    EXECUTE format('ALTER TABLE %s DROP COLUMN lp_id;', tb);
    EXECUTE format('ALTER TABLE %s DROP COLUMN alphavalue;', tb);
    EXECUTE format('ALTER TABLE %s DROP COLUMN nComplete;', tb);
    EXECUTE format('ALTER TABLE %s DROP COLUMN Ibitmap;', tb);

--  drop buckets
    FOR rec IN EXECUTE format('SELECT bucketid FROM %s;', tb || '_LATMP;')
    LOOP
        EXECUTE format('DROP TABLE %s;', 'lp_' || tb || '_' || rec.bucketid);
    END LOOP;

--  drop the temporary table for Lattices
    EXECUTE format('DROP TABLE %s;', tb || '_LATMP');

--  drop triggers
    EXECUTE format('DROP TRIGGER %s_lains ON %s', tb, tb);
    EXECUTE format('DROP FUNCTION lp_%s_triins()', tb);
    EXECUTE format('DROP TRIGGER %s_ladel ON %s', tb, tb);
    EXECUTE format('DROP FUNCTION lp_%s_tridel()', tb);
    EXECUTE format('DROP TRIGGER %s_laupd ON %s', tb, tb);
    EXECUTE format('DROP FUNCTION lp_%s_triupd()', tb);

END
$T1$
language plpgsql;
