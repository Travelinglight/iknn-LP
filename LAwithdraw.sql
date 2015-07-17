CREATE FUNCTION LAwithdraw(tb text) RETURNS void
AS $$
DECLARE
    tmptb text; -- the tmp table name
    delCalpha text; -- delete column alphavalue from tb
    delCl text; -- delelete column lattice key, the number of incomplete attributes, from tb
    delCb text; -- delelete column bucket key, the bitmap of incomplete attributes, from tb
    Dtb text;   -- Ctb is the command to delete the temporary table
--    selectALL text; -- select all tuples from the target table for updating
BEGIN
--  alter the target table
    delCalpha := 'ALTER TABLE ' || tb || ' DROP COLUMN alphavalue;';
    delCl := 'ALTER TABLE ' || tb || ' DROP COLUMN nIncomplete;';
    delCb := 'ALTER TABLE ' || tb || ' DROP COLUMN Ibitmap;';
    execute delCalpha;
    execute delCl;
    execute delCb;

--  create a temporary table for Lattices
    tmptb := tb || '_LATMP';
    Dtb := 'DROP TABLE ' || tmptb;
    execute Dtb;

--  drop function (for debugging)
    execute 'DROP FUNCTION lainit(text);';
END
$$
language plpgsql;
