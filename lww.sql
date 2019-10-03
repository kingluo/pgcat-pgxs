CREATE or replace FUNCTION foobar_lww_trigger() RETURNS trigger AS $$
declare
	v_new_ts timestamp;
	v_ts_field text = 'ts:row';
	v_tumbstone_field text = '__deleted';
	v_is_replica boolean = false;
	v_sysid bigint;
	v_col5_val_field text;
	v_name2 text;
	v_should_update boolean;
	v_counter text;
	v_counters text[];
	v_lww jsonb;
BEGIN
	if TG_NARGS > 0 then
		v_is_replica = TG_ARGV[0]::boolean;
	end if;

	select system_identifier from pg_control_system() into v_sysid;

	v_col5_val_field = 'val:col5@' || v_sysid;

	case TG_OP
		when 'INSERT' then
			select _lww into v_lww from foobar where id = NEW.id for update;
			if found then
				if v_is_replica or v_lww ? v_tumbstone_field then
					update foobar set (id, col1, col2, col3, col4, col5, _lww) = (select NEW.*);
					return NULL;
				else
					return NEW;
				end if;
			else
				if not v_is_replica then
					v_new_ts = clock_timestamp();
					NEW._lww = ('{"' || v_ts_field || '": "' || v_new_ts::text || '"}')::jsonb;

					-- col2, normal column
					NEW._lww = NEW._lww || ('{"ts:col1": "' || v_new_ts::text || '"}')::jsonb;

					-- col5, counter column
					NEW._lww = NEW._lww || ('{"' || v_col5_val_field || '": ' || NEW.col5 || '}')::jsonb;
				end if;
				-- retain our systemid
				NEW._lww = NEW._lww || ('{"sysid": ' || v_sysid || '}')::jsonb;
				return NEW;
			end if;
		when 'UPDATE' then
			if v_is_replica then
				v_new_ts = (NEW._lww->>v_ts_field)::timestamp;
			else
				v_new_ts = clock_timestamp();
			end if;

			v_should_update = false;

			-- col2, normal column
			if v_is_replica then
				if (NEW._lww->>'ts:col1')::timestamp < (OLD._lww->>'ts:col1')::timestamp then
					NEW._lww = NEW._lww || ('{"ts:col1": "' || (OLD._lww->>'ts:col1')::text || '"}')::jsonb;
					NEW.col1 = OLD.col1;
				else
					v_should_update = true;
				end if;
			else
				if NEW.col1 is distinct from OLD.col1 then
					v_should_update = true;
					NEW._lww = NEW._lww || ('{"ts:col1": "' || v_new_ts::text || '"}')::jsonb;
				end if;
			end if;

			-- col5, counter column
			if v_is_replica then
				v_should_update = true;

				v_name2 = 'val:col5@' || (NEW._lww->>'sysid')::text;
				-- remove other counters
				select array_agg(key) into v_counters from jsonb_each_text(NEW._lww) where key like 'val:col5@%';
				foreach v_counter in array v_counters loop
					if v_counter != v_col5_val_field and v_counter != v_name2 then
						NEW._lww = NEW._lww - v_counter;
					end if;
				end loop;

				-- retain myself
				NEW._lww = NEW._lww || ('{"' || v_col5_val_field || '": ' || (OLD._lww->>v_col5_val_field)::text || '}')::jsonb;

				-- keep local counter unchanged
				NEW.col5 = OLD.col5;
			else
				if NEW.col5 is distinct from OLD.col5 then
					v_should_update = true;
					NEW._lww = NEW._lww || ('{"' || v_col5_val_field || '": ' || NEW.col5 || '}')::jsonb;
				end if;
			end if;

			if v_should_update or v_new_ts > (OLD._lww->>v_ts_field)::timestamp then
				if v_new_ts > (OLD._lww->>v_ts_field)::timestamp then
					NEW._lww = NEW._lww ||
						('{"' || v_ts_field || '": "' || v_new_ts::text || '"}')::jsonb;
				else
					NEW._lww = NEW._lww ||
						('{"' || v_ts_field || '": "' || (OLD._lww->>v_ts_field)::text || '"}')::jsonb;
				end if;

				-- retain our systemid
				NEW._lww = NEW._lww || ('{"sysid": ' || v_sysid || '}')::jsonb;
				return NEW;
			else
				raise notice 'ignore update';
				return NULL;
			end if;
		when 'DELETE' then
			if v_is_replica then
				raise notice 'ignore delete from replica';
			end if;

			if current_setting('lww.force_delete', true) is null then
				if OLD._lww ? v_tumbstone_field then
					raise notice 'ignore duplicated delete';
					return NULL;
				end if;
				update foobar set _lww = OLD._lww ||
					('{"' || v_tumbstone_field || '": true}')::jsonb
					where id = OLD.id;
				return NULL;
			else
				return OLD;
			end if;
	end case;

	return NULL;
END;
$$ LANGUAGE plpgsql;

/*
CREATE TRIGGER foobar_lww_trigger_local
	BEFORE INSERT OR UPDATE OR DELETE ON foobar
	FOR EACH ROW EXECUTE FUNCTION foobar_lww_trigger();

CREATE TRIGGER foobar_lww_trigger_replica
	BEFORE INSERT OR UPDATE OR DELETE ON foobar
	FOR EACH ROW EXECUTE FUNCTION foobar_lww_trigger(true);
alter table foobar disable trigger foobar_lww_trigger_replica;
alter table foobar enable replica trigger foobar_lww_trigger_replica;
*/
