SELECT film.film_id 
FROM film 
WHERE film.film_id = 111;

SELECT film_actor.film_id 
FROM film_actor
WHERE film_actor.film_id = 111;

SELECT film_category.film_id 
FROM film_category
WHERE film_category.film_id = 111;

SELECT inventory.inventory_id 
FROM inventory 
WHERE inventory.film_id = 111;

SELECT payment.payment_id
FROM payment, rental, inventory 
WHERE payment.rental_id = rental.rental_id and 
      rental.inventory_id = inventory.inventory_id and 
      inventory.film_id = 111;

SELECT rental.rental_id 
FROM rental, inventory 
WHERE rental.inventory_id = inventory.inventory_id and 
      inventory.film_id = 111;





