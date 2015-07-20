CREATE EXTENSION hstore;

CREATE FUNCTION LAinit(tb text) RETURNS void
AS $$
DECLARE
    nColumn int;    -- the number of columns of the target table
    vals text;   -- temp use
BEGIN
--  get the number of columns of the target table
    EXECUTE format('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=%s;', quote_nullable(tb)) INTO nColumn;

--  alter the target table
    EXECUTE format('ALTER TABLE %s ADD COLUMN alphavalue float;', tb);
    EXECUTE format('ALTER TABLE %s ADD COLUMN nIncomplete int;', tb);
    EXECUTE format('ALTER TABLE %s ADD COLUMN Ibitmap varchar(%s);', tb, nColumn::text);

--  create a temporary table for Lattices
    EXECUTE format('CREATE TABLE %s(latticeID int, bucketID varchar(%s));', tb || '_LATMP', nColumn::text);
    EXECUTE format('CREATE INDEX hash_%s ON %s USING HASH (latticeID);', tb || '_LATMP', tb || '_LATMP');

--  update nIncomplete of the target table
    SELECT string_agg(format('(%s is null)::int',column_name), '+') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'nincomplete') and (column_name != 'ibitmap'));
    EXECUTE format('UPDATE %s SET nIncomplete = (%s)', tb, vals);

-- update the alphavalue of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN 0 ELSE %s END', column_name, column_name), '+') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'nincomplete') and (column_name != 'ibitmap'));
    vals := format('(%s)::float / (%s - nincomplete)', vals::text, nColumn);
    EXECUTE format('UPDATE %s SET alphavalue = (%s)', tb, vals);

-- update the Ibitmap of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN ''0'' ELSE ''1'' END', column_name, column_name), ' || ') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'nincomplete') and (column_name != 'ibitmap'));
    EXECUTE format('UPDATE %s SET ibitmap = (%s)', tb, vals);

--  add hash index to the target table
    EXECUTE format('CREATE INDEX hash_%s ON %s USING HASH (nincomplete);', tb, tb);

-- update the _LATMP table;
    FOR vals IN EXECUTE format('SELECT distinct format(''INSERT INTO %s VALUES(%%s, %%s);'', nincomplete::text, quote_nullable(ibitmap)) FROM %s;', tb || '_LATMP', tb) LOOP
    EXECUTE vals;
    END LOOP;

-- create trigger for insertion/updating
EXECUTE format('
CREATE FUNCTION LP_%s_triInUp() RETURNS TRIGGER 
AS $T2$
DECLARE
    r record;
    nin int := 0;
    nco int := 0;
    alp float := 0;
    bit text := '''';
BEGIN
-- compute the three additional attributes
    FOR r IN SELECT (each(hstore(NEW))).*
    LOOP
        IF r.key = ''alphavalue'' THEN CONTINUE; END IF;
        IF r.key = ''nincomplete'' THEN CONTINUE; END IF;
        IF r.key = ''ibitmap'' THEN CONTINUE; END IF;
        IF r.value is null THEN
            nin := nin + 1;
            bit := bit || ''0'';
        ELSE
            nco := nco + 1;
            alp := alp + r.value::int;
            bit := bit || ''1'';
        END IF;
    END LOOP;
    alp := alp / nco;
    NEW.nincomplete := nin;
    NEW.alphavalue := alp;
    NEW.ibitmap := bit;
    
-- update lattice table
    EXECUTE format(''INSERT INTO %s_latmp SELECT %%s, %%s WHERE NOT EXISTS ( SELECT * FROM hash_latmp WHERE latticeid = %%s and bucketid = %%s);'', nin, quote_nullable(bit), nin, quote_nullable(bit));
    RETURN NEW;
END
$T2$ LANGUAGE plpgsql;', tb, tb);
EXECUTE format('CREATE TRIGGER %s_LAinup BEFORE INSERT OR UPDATE ON %s
FOR EACH ROW EXECUTE PROCEDURE LP_%s_triInUp();', tb, tb, tb);

END
$$
language plpgsql;
