create table if not exists offers
(
    offer_id bigint not null,
    modifydate timestamp with time zone,
    constraint offers_pkey primary key (offer_id)
);

create table if not exists offers_detail
(
    offer_id bigint not null,
    prop character varying(255) not null,
    val text,
    constraint offers_detail_pkey primary key (offer_id, prop)
);

create table if not exists offers_detail_history
(
    offer_id bigint not null,
    prop character varying(255) not null,
	modifydate timestamp not null,
    val text,
    constraint offers_detail_history_pkey primary key (offer_id, prop, modifydate)
);

create or replace function f_offers_detail()
    returns trigger
    language 'plpgsql'
    cost 100
    volatile not leakproof
as $body$
begin
	if (old.val is distinct from new.val) then	
	  insert into offers_detail_history (offer_id, prop, modifydate, val)
	  select old.offer_id, old.prop, modifydate, old.val
	  from offers
	  where offer_id = new.offer_id;
	end if;
	return new;
end; 
$body$;

create trigger offers_detail_au after update on offers_detail
    for each row execute function f_offers_detail();