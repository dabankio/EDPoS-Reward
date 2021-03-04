select amount from Tx 
where type='certification' and n=0 and height < 248997;


select * from Block where height=248997; -- hash: 0003cca552ae27b26db2339c8180b007c2ded18f92027fe381a30f3cca16262f
select * from Tx where block_hash='0003cca552ae27b26db2339c8180b007c2ded18f92027fe381a30f3cca16262f'; --min id:1129517

select count(*) from Tx 
where type='certification' and n=0 and id < 1129517; --19920


select id,amount from Tx 
where type='certification' and n=0 and id < 1129517
order by id desc limit 20;

select * from Tx 
where type='certification' and n=0 and id < 1129517
order by id desc limit 20;