select
o.order_date, 
O.order_id,
sum(total_price) AS total_price 
from 
finance_db."row".stage_orders O 
LEFT JOIN finance_db."row".stage_order_items OI
ON O.ORDER_ID=OI.ORDER_ID
GROUP BY 1,2