CREATE FUNCTION LPwithdraw(tb text) RETURNS void
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
    EXECUTE format('DROP TRIGGER %s_lainup ON %s;', tb, tb);
    EXECUTE format('DROP FUNCTION lp_%s_triinup();', tb);
    EXECUTE format('DROP TRIGGER %s_ladel ON %s', tb, tb);
    EXECUTE format('DROP FUNCTION lp_%s_tridel();', tb);

--  drop function (for debugging)
    EXECUTE 'DROP FUNCTION lpinit(text);';

--  drop extension (for debugging)
    DROP EXTENSION hstore;
END
$T1$
language plpgsql;
