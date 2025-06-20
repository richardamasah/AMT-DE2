DROP TABLE IF EXISTS raw_layer.apartment_attributes;

CREATE TABLE raw_layer.apartment_attributes (
    id INT,
    category VARCHAR(100),
    body VARCHAR(500),
    amenities VARCHAR(500),
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(100),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(500),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7)
);


DROP TABLE IF EXISTS raw_layer.apartments;
CREATE TABLE raw_layer.apartments (
    id INT,
    title VARCHAR(255),
    source VARCHAR(100),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    listing_created_on TIMESTAMP,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS raw_layer.user_viewing;
CREATE TABLE raw_layer.user_viewing (
    user_id INT,
    apartment_id INT,
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(100)
);

DROP TABLE IF EXISTS raw_layer.bookings;
CREATE TABLE raw_layer.bookings (
    booking_id INT,
    user_id INT,
    apartment_id INT,
    booking_date TIMESTAMP,
    checkin_date TIMESTAMP,
    checkout_date TIMESTAMP,
    total_price DECIMAL(10,2),
    currency VARCHAR(10),
    booking_status VARCHAR(50)
);