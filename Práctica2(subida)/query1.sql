SELECT rental.customer_id, rental.rental_id, inventory.inventory_id, payment.payment_id, rental.rental_date, rental.return_date, rental.staff_id, payment.amount
FROM inventory, rental, payment
WHERE rental.customer_id = 39 and
      rental.inventory_id = inventory.inventory_id and
      rental.rental_id = payment.rental_id and
      inventory.film_id = 1 and
      inventory.store_id = 1;
