CREATE EXTENSION hstore;

CREATE FUNCTION LPinit(tb text) RETURNS void
AS $$
DECLARE
    nColumn int;    -- the number of columns of the target table
    vals text;  -- temp use
    rec record; -- for looping through selects
    reccol record;  -- for looping through a record
    bktcol text := '';    -- columns of buckets
    bktinsk text := '';
    bktinsv text := '';
BEGIN
--  get the number of columns of the target table
    EXECUTE format('SELECT COUNT(*) FROM information_schema.columns WHERE table_name=%s;', quote_nullable(tb)) INTO nColumn;

--  alter the target table
    EXECUTE format('ALTER TABLE %s ADD COLUMN lp_id SERIAL', tb);
    EXECUTE format('ALTER TABLE %s ADD COLUMN alphavalue FLOAT;', tb);
    EXECUTE format('ALTER TABLE %s ADD COLUMN nComplete INT;', tb);
    EXECUTE format('ALTER TABLE %s ADD COLUMN Ibitmap VARCHAR(%s);', tb, nColumn::text);

--  create a temporary table for Lattices
    EXECUTE format('CREATE TABLE %s(latticeID int, bucketID varchar(%s));', tb || '_LATMP', nColumn::text);
    EXECUTE format('CREATE INDEX hash_%s ON %s USING HASH (latticeID);', tb || '_LATMP', tb || '_LATMP');

--  update nComplete of the target table
    SELECT string_agg(format('(%s is null)::int',column_name), '+') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'ncomplete') and (column_name != 'ibitmap') and (column_name != 'lp_id'));
    vals := format('%s - (%s)', nColumn::text, vals);
    EXECUTE format('UPDATE %s SET nComplete = (%s)', tb, vals);

-- update the alphavalue of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN 0 ELSE %s END', column_name, column_name), '+') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'ncomplete') and (column_name != 'ibitmap') and (column_name != 'lp_id'));
    vals := format('(%s)::float / ncomplete', vals::text);
    EXECUTE format('UPDATE %s SET alphavalue = (%s)', tb, vals);

-- update the Ibitmap of the target table
    SELECT string_agg(format('CASE WHEN %s is null THEN ''0'' ELSE ''1'' END', column_name, column_name), ' || ') INTO vals FROM information_schema.columns WHERE ((table_name = ''||tb||'') and (column_name != 'alphavalue') and (column_name != 'ncomplete') and (column_name != 'ibitmap') and (column_name != 'lp_id'));
    EXECUTE format('UPDATE %s SET ibitmap = (%s)', tb, vals);

--  add hash index to the target table
    EXECUTE format('CREATE INDEX hash_%s ON %s USING HASH (ibitmap);', tb, tb);

-- update the _LATMP table;
    FOR vals IN EXECUTE format('SELECT distinct format(''INSERT INTO %s VALUES(%%s, %%s);'', ncomplete::text, quote_nullable(ibitmap)) FROM %s;', tb || '_LATMP', tb)
    LOOP
        EXECUTE vals;
    END LOOP;

--  build buckets
    -- get bucket columns
    FOR rec IN EXECUTE format('SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s', quote_nullable(tb))
    LOOP
        IF rec.column_name = 'ncomplete' THEN CONTINUE; END IF;
        IF rec.column_name = 'ibitmap' THEN CONTINUE; END IF;
        IF rec.column_name = 'lp_id' THEN
            IF bktcol != '' THEN
                bktcol := bktcol || ',';
            END IF;
            bktcol := bktcol || 'lp_id int';
        ELSE
            IF bktcol != '' THEN
                bktcol := bktcol || ',';
            END IF;
            bktcol := bktcol || rec.column_name || ' ';
            IF rec.data_type = 'double precision' OR rec.data_type = 'single precision' THEN
                bktcol := bktcol || 'float';
            ELSE
                bktcol := bktcol || rec.data_type;
            END IF;
        END IF;
    END LOOP;

    -- start to build and update buckets
    FOR rec IN EXECUTE format('SELECT * FROM %s', tb)
    LOOP
        EXECUTE format('SELECT * FROM pg_catalog.pg_tables WHERE tablename = %s;', quote_nullable('lp_' || tb || '_' || rec.ibitmap)) INTO vals;
        IF vals THEN
        ELSE
            EXECUTE format('CREATE TABLE %s(%s);', 'lp_' || tb || '_' || rec.ibitmap, bktcol);
            EXECUTE format('CREATE INDEX ON %s USING BTREE (alphavalue);', 'lp_' || tb || '_' || rec.ibitmap);
        END IF;

        bktinsk := '';
        bktinsv := '';
        FOR reccol IN SELECT (each(hstore(rec))).*
        LOOP
            IF reccol.key = 'ibitmap' THEN CONTINUE; END IF;
            IF reccol.key = 'ncomplete' THEN CONTINUE; END IF;
            IF reccol.value is null THEN
            ELSE
                IF bktinsk = '' THEN
                    bktinsk = format('INSERT INTO %s(%s', 'lp_' || tb || '_' || rec.ibitmap, reccol.key);
                    bktinsv = format('VALUES(%s', reccol.value::text);
                ELSE
                    bktinsk := bktinsk || ', ' || reccol.key;
                    bktinsv := bktinsv || ', ' || reccol.value::text;
                END IF;
            END IF;
        END LOOP;
        bktinsk := bktinsk || ')';
        bktinsv := bktinsv || ')';
        EXECUTE bktinsk || bktinsv;
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
    vals text := '''';
    bktinsk text := '''';
    bktinsv text := '''';
BEGIN
-- compute the three additional attributes
    FOR r IN SELECT (each(hstore(NEW))).*
    LOOP
        IF r.key = ''lp_id'' THEN CONTINUE; END IF;
        IF r.key = ''alphavalue'' THEN CONTINUE; END IF;
        IF r.key = ''ncomplete'' THEN CONTINUE; END IF;
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
    NEW.ncomplete := nco;
    NEW.alphavalue := alp;
    NEW.ibitmap := bit;

    EXECUTE format(''SELECT * FROM pg_catalog.pg_tables WHERE tablename = %%s;'', quote_nullable(''lp_%s_'' || NEW.ibitmap)) INTO vals;
    IF vals = '''' THEN
        EXECUTE format(''CREATE TABLE %%s(%s);'', ''lp_%s_'' || NEW.ibitmap);
    END IF;

    FOR r IN SELECT (each(hstore(NEW))).*
    LOOP
        IF r.key = ''ibitmap'' THEN CONTINUE; END IF;
        IF r.key = ''ncomplete'' THEN CONTINUE; END IF;
        IF r.value is null THEN
        ELSE
            IF bktinsk = '''' THEN
                bktinsk = format(''INSERT INTO %%s(%%s'', ''lp_%s_'' || NEW.ibitmap, r.key);
                bktinsv = format(''VALUES(%%s'', r.value::text);
            ELSE
                bktinsk := bktinsk || '', '' || r.key;
                bktinsv := bktinsv || '', '' || r.value::text;
            END IF;
        END IF;
    END LOOP;
    bktinsk := bktinsk || '')'';
    bktinsv := bktinsv || '')'';
    EXECUTE bktinsk || bktinsv;

-- update lattice table
    EXECUTE format(''INSERT INTO %s_latmp SELECT %%s, %%s WHERE NOT EXISTS ( SELECT * FROM %s_latmp WHERE latticeid = %%s and bucketid = %%s);'', nin, quote_nullable(bit), nin, quote_nullable(bit));
    RETURN NEW;
END
$T2$ LANGUAGE plpgsql;', tb, tb, bktcol, tb, tb, tb, tb);
EXECUTE format('CREATE TRIGGER %s_LAinup BEFORE INSERT OR UPDATE ON %s
FOR EACH ROW EXECUTE PROCEDURE LP_%s_triInUp();', tb, tb, tb);

EXECUTE format('
CREATE FUNCTION LP_%s_tridel() RETURNS TRIGGER
AS $T1$
DECLARE nexist int;
BEGIN
    EXECUTE format(''DELETE FROM lp_%s_%%s WHERE lp_id = %%s'', OLD.ibitmap, OLD.lp_id);
    EXECUTE format(''SELECT count(*) FROM %s WHERE ncomplete = %%s and ibitmap = %%s'', OLD.ncomplete::text, quote_nullable(OLD.ibitmap)) INTO nexist;
    IF nexist = 0 THEN
        EXECUTE format(''DELETE FROM %s_latmp WHERE latticeid = %%s and bucketid = %%s;'', OLD.ncomplete::text, quote_nullable(OLD.ibitmap));
        EXECUTE format(''DROP TABLE lp_%s_%%s;'', OLD.ibitmap);
    END IF;
    RETURN OLD;
END
$T1$ LANGUAGE plpgsql;', tb, tb, tb, tb, tb);

EXECUTE format(' CREATE TRIGGER %s_LAdel AFTER DELETE ON %s
FOR EACH ROW EXECUTE PROCEDURE LP_%s_tridel();', tb, tb, tb);

END
$$
language plpgsql;
