-- grant
grant usage on schema pgcat to pgcat;

-- tables

create table if not exists pgcat_subscription(
	name text primary key,
	hostname text not null,
	port int not null,
	username text not null,
	password text not null,
	dbname text not null,
	publications text[] not null,
	copy_data boolean default true,
	enabled boolean default true
);

alter table pgcat_subscription owner to pgcat;
SELECT pg_catalog.pg_extension_config_dump('pgcat_subscription', '');

create table if not exists pgcat_subscription_progress(
	subscription text primary key references pgcat_subscription(name) ON DELETE CASCADE,
	lsn pg_lsn not null
);

alter table pgcat_subscription_progress owner to pgcat;

create table if not exists pgcat_table_mapping(
	subscription text references pgcat_subscription(name) ON DELETE CASCADE,
	priority int not null,
	src text not null,
	dst text not null
);

alter table pgcat_table_mapping owner to pgcat;
SELECT pg_catalog.pg_extension_config_dump('pgcat_table_mapping', '');

create table if not exists pgcat_replident(
	tablename text primary key,
	columns text[] not null
);

alter table pgcat_replident owner to pgcat;
SELECT pg_catalog.pg_extension_config_dump('pgcat_replident', '');

create table if not exists pgcat_subscription_rel(
	subscription text references pgcat_subscription(name) ON DELETE CASCADE,
	remotetable text not null,
	localtable text not null,
	-- State code:
	-- i = initialize, d = data is being copied,
	-- c = catching up, r = ready (normal replication)
	state char not null,
	primary key(subscription, remotetable, localtable)
);

alter table pgcat_subscription_rel owner to pgcat;

-- trigger

CREATE or replace FUNCTION pgcat_cfg_trigger() RETURNS trigger AS $$
declare
	v_row record = case when TG_OP = 'DELETE' then OLD else NEW end;
BEGIN
	if TG_RELNAME = 'pgcat_subscription' then
		if TG_OP = 'UPDATE' and OLD.name != NEW.name then
			raise 'cannot change name';
		end if;
		PERFORM pg_notify('pgcat_cfg_changed', TG_OP || ' ' || v_row.name);
	elsif TG_RELNAME = 'pgcat_table_mapping' then
		if TG_OP = 'UPDATE' and OLD.subscription != NEW.subscription then
			raise 'cannot change subscription';
		end if;
		PERFORM pg_notify('pgcat_cfg_changed', TG_OP || ' ' || v_row.subscription);
	end if;
	return v_row;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pgcat_subscription_cfg_trigger
	BEFORE INSERT OR UPDATE OR DELETE ON pgcat_subscription
	FOR EACH ROW EXECUTE FUNCTION pgcat_cfg_trigger();

CREATE TRIGGER pgcat_table_mapping_cfg_trigger
	BEFORE INSERT OR UPDATE OR DELETE ON pgcat_table_mapping
	FOR EACH ROW EXECUTE FUNCTION pgcat_cfg_trigger();

-- functions

CREATE or replace FUNCTION pgcat_lock_and_check_table_insync(text, text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION pgcat_lock_and_check_table_insync(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_lock_and_check_table_insync(text, text) TO pgcat;

CREATE or replace FUNCTION pgcat_set_table_insync(oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;
REVOKE ALL ON FUNCTION pgcat_set_table_insync(oid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_set_table_insync(oid) TO pgcat;

CREATE or replace FUNCTION pgcat_check_table(v_ns text, v_name text)
RETURNS boolean AS $$
declare
	v_tablename text = v_ns || '.' || v_name;
	v_lastxmin xid;
	v_xmin xid;
BEGIN
	-- check table if insync first
	if not pgcat_lock_and_check_table_insync(v_ns, v_name) then
		return false;
	end if;

	-- get current xmin
	select xmin from pgcat_replident where tablename = v_tablename for update into v_xmin;
	if not found then
		return true;
	end if;

	-- get last recorded xmin
	begin
		select lastxmin from pgcat_tmp1 where ns=v_ns and name=v_name into v_lastxmin;
	exception when undefined_table then
		raise notice 'create tmp table';
		EXECUTE 'create temporary table pgcat_tmp1(ns text not null, name text not null, lastxmin xid not null)';
		EXECUTE 'create unique index pgcat_tmp1_idx1 on pgcat_tmp1(ns, name)';
		v_lastxmin = 0;
	end;

	-- compare
	if v_lastxmin != v_xmin then
		return false;
	end if;

	return true;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION pgcat_get_table_columns(v_ns text, v_name text)
RETURNS table(col1 text, col2 bool) AS $$
declare
	v_record record;
	v_tablename text = v_ns || '.' || v_name;
	v_record2 record;
BEGIN
	select oid, relkind, relreplident from pg_class where oid = v_tablename::regclass into v_record;
	case v_record.relkind
		when 'r', 'p' then
			case v_record.relreplident
				when 'd' then
					return query select attname::text, ARRAY[attnum] <@ indkey from pg_attribute,
							(SELECT indrelid, string_to_array(indkey::text, ' ')::int2[] as indkey
								FROM pg_index WHERE indrelid = v_tablename::regclass and indisprimary=true) as t
							where indrelid = attrelid and attnum > 0 and attisdropped=false order by attnum;
				when 'i' then
					return query select attname::text, ARRAY[attnum] <@ indkey from pg_attribute,
							(SELECT indrelid, string_to_array(indkey::text, ' ')::int2[] as indkey
								FROM pg_index WHERE indrelid = v_tablename::regclass and indisreplident=true) as t
							where indrelid = attrelid and attnum > 0 and attisdropped=false order by attnum;
				else
					raise 'replica identity must be pk or index, table=%', v_tablename;
			end case;
		when 'f', 'v' then
			SELECT xmin, columns from pgcat_replident where tablename = v_tablename into v_record2;
			if NOT FOUND then
				raise 'no replica ident';
			end if;

			begin
				insert into pgcat_tmp1 values(v_ns, v_name, v_record2.xmin)
					on conflict (ns, name) do update set lastxmin = excluded.lastxmin;
				raise notice 'insert tmp table';
			exception when undefined_table then
				raise notice 'create tmp table';
				EXECUTE 'create temporary table pgcat_tmp1(ns text not null, name text not null, lastxmin xid not null)';
				EXECUTE 'create unique index pgcat_tmp1_idx1 on pgcat_tmp1(ns, name)';
				insert into pgcat_tmp1 values(v_ns, v_name, v_record2.xmin)
					on conflict (ns, name) do update set lastxmin = excluded.lastxmin;
			end;

			return query select attname::text, ARRAY[attname::text] <@ v_record2.columns from pg_attribute
					where attrelid = v_record.oid and attnum > 0 and attisdropped=false;
		else
			raise 'unsupported relkind=%', v_record.relkind;
	end case;

	if NOT FOUND then
		raise 'no columns found';
	end if;

	perform pgcat_set_table_insync(v_tablename::regclass);

	return;
END;
$$ LANGUAGE plpgsql;

CREATE or replace FUNCTION pgcat_replication_origin_oid(name text) RETURNS oid AS $$
select pg_replication_origin_oid(name);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_oid(name text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_oid(name text) TO pgcat;

CREATE or replace FUNCTION pgcat_replication_origin_create(name text) RETURNS oid AS $$
select pg_replication_origin_create(name);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_create(name text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_create(name text) TO pgcat;

CREATE or replace FUNCTION pgcat_replication_origin_drop(name text) RETURNS void AS $$
select pg_replication_origin_drop(name);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_drop(name text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_drop(name text) TO pgcat;

CREATE or replace FUNCTION pgcat_set_session_replication_role() RETURNS void AS $$
select set_config('session_replication_role', 'replica', false);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_set_session_replication_role() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_set_session_replication_role() TO pgcat;

CREATE or replace FUNCTION pgcat_replication_origin_session_setup(name text) RETURNS void AS $$
select pg_replication_origin_session_setup(name);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_session_setup(name text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_session_setup(name text) TO pgcat;

CREATE or replace FUNCTION pgcat_replication_origin_session_reset() RETURNS void AS $$
select pg_replication_origin_session_reset();
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_session_reset() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_session_reset() TO pgcat;

CREATE or replace FUNCTION pgcat_replication_origin_xact_setup(origin_lsn pg_lsn, origin_timestamp timestamptz) RETURNS void AS $$
select pg_replication_origin_xact_setup(origin_lsn, origin_timestamp);
$$ LANGUAGE sql
SECURITY DEFINER;
REVOKE ALL ON FUNCTION pgcat_replication_origin_xact_setup(origin_lsn pg_lsn, origin_timestamp timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgcat_replication_origin_xact_setup(origin_lsn pg_lsn, origin_timestamp timestamptz) TO pgcat;
