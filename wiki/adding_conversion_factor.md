To add 
mysql -h fixed-income1.clmdxgxhslqn.us-east-1.rds.amazonaws.com -u cvmysql -p -A
use risk_parity
insert into tick_conversion values ('DJ','10','USD');

To find out information in cvfif DB:
select * from products where Product='DJ' ;