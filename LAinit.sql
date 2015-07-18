CREATE FUNCTION LAinit(tb text) RETURNS void
AS $$
DECLARE
    nColumn int;    -- the number of columns of the target table
    vals text;   -- temp use
BEGIN
--  get the number of columns of the target table
    execute format('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=%s;', quote_nullable(tb)) INTO nColumn;

--  alter the target table
    execute format('ALTER TABLE %s ADD COLUMN alphavalue float;', tb);
    execute format('ALTER TABLE %s ADD COLUMN nIncomplete int;', tb);
    execute format('ALTER TABLE %s ADD COLUMN Ibitmap varchar(%s);', tb, nColumn::text);

--  create a temporary table for Lattices
    execute format('CREATE TABLE %s(latticeID int, bucketID varchar(%s));', tb || '_LATMP', nColumn::text);
    execute format('CREATE INDEX tmphash ON %s USING HASH (latticeID);', tb || '_LATMP');

--  update nIncomplete of the target table
    SELECT string_agg(format('(%s is null)::int',column_name), '+') INTO vals FROM information_schema.columns WHERE table_name = ''||tb||'';
    vals := vals || '-3';
    EXECUTE format('UPDATE %s SET nIncomplete = (%s)', tb, vals);

-- update the alphavalue of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN 0 ELSE %s END', column_name, column_name), '+') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'nincomplete') and (column_name != 'ibitmap'));
    vals := format('(%s)::float / (%s - nincomplete)', vals::text, nColumn);
    EXECUTE format('UPDATE %s SET alphavalue = (%s)', tb, vals);

-- update the Ibitmap of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN ''0'' ELSE ''1'' END', column_name, column_name), ' || ') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'nincomplete') and (column_name != 'ibitmap'));
    EXECUTE format('UPDATE %s SET ibitmap = (%s)', tb, vals);

-- update the _LATMP table;
    FOR vals IN EXECUTE format('SELECT distinct format(''INSERT INTO %s VALUES(%%s, %%s);'', nincomplete::text, quote_nullable(ibitmap)) FROM %s;', tb || '_LATMP', tb) LOOP
        EXECUTE vals;
    END LOOP;
END
$$
language plpgsql;
