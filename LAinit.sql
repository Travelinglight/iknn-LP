CREATE FUNCTION LAinit(tb text) RETURNS void
AS $$
DECLARE
    tmptb text; -- the tmp table name
    nColumn int;    -- the number of columns of the target table
    cName text;  -- column name of the original table
    getNC text; -- get nColumn;
    addCalpha text; -- add column alphavalue to tb
    addCl text; -- add column lattice key, the number of incomplete attributes, to tb
    addCb text; -- add column bucket key, the bitmap of incomplete attributes, to tb
    Ctb text;   -- Ctb is the command to create a table
    tbhash text;    -- set up hash index on the created table
    vals text;   -- counting the number of incomplete attributes
BEGIN
--  get the number of columns of the target table
    getNC := 'SELECT COUNT(*) FROM information_schema.columns WHERE table_name=' || quote_nullable(tb) || ';';
    execute getNC INTO nColumn;

--  alter the target table
    addCalpha := 'ALTER TABLE ' || tb || ' ADD COLUMN alphavalue float;';
    addCl := 'ALTER TABLE ' || tb || ' ADD COLUMN nIncomplete int;';
    addCb := 'ALTER TABLE ' || tb || ' ADD COLUMN Ibitmap varchar(' || nColumn::text  || ');';
    execute addCalpha;
    execute addCl;
    execute addCb;

--  create a temporary table for Lattices
    tmptb := tb || '_LATMP';
    Ctb := 'CREATE TABLE ' || tmptb || '(latticeID int, bucketID varchar(128));';
    tbhash := 'CREATE INDEX tmphash ON ' || tmptb || ' USING HASH (latticeID);';
    execute Ctb;
    execute tbhash;

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

END
$$
language plpgsql;
