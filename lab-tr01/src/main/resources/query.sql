SELECT
	c.name AS name,
	a.account_number AS account_number,
	a.ledger_account AS ledger,
	a.id    AS id
FROM customers c
INNER JOIN accounts a
ON c.id = a.customer_id