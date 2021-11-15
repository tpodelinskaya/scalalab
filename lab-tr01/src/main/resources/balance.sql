SELECT DISTINCT
	b.account_id AS id,
	FIRST_VALUE(b.balance_out_rub) OVER (PARTITION BY b.account_id ORDER BY b.on_date DESC) AS act_balance
FROM balance b
ORDER BY b.account_id
