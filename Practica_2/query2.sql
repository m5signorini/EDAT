SELECT rental.rental_id, payment.payment_id
FROM rental, payment
WHERE rental.rental_id = payment.rental_id and
      rental.rental_id = 10126
