create table eco_codes(
id            serial primary key,
eco_code      text   not null,
opening_name  text   not null,
opening_notes text   not null
);