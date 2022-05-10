SELECT o_orderkey, sum(o_custkey), sum(o_orderstatus)
FROM orders
GROUP BY o_orderkey, o_custkey, o_orderstatus
order by o_orderkey
LIMIT 10
