CREATE DATABASE `db_purchase`;

CREATE TABLE `orders` (
  `order_number` varchar(50) DEFAULT NULL,
  `order_status` varchar(50) DEFAULT NULL,
  `customer_email` varchar(255) DEFAULT NULL,
  `preferred_delivery_date` datetime DEFAULT NULL,
  `preferred_delivery_hours` varchar(50) DEFAULT NULL,
  `sales_person` varchar(50) DEFAULT NULL,
  `notes` text,
  `address` varchar(255) DEFAULT NULL,
  `neighbourhood` varchar(50) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL,
  `creation_date` datetime DEFAULT NULL,
  `source` varchar(50) DEFAULT NULL,
  `warehouse` varchar(50) DEFAULT NULL,
  `shopify_id` decimal(18,0) DEFAULT NULL,
  `sales_person_role` varchar(50) DEFAULT NULL,
  `order_type` varchar(50) DEFAULT NULL,
  `is_pitayas` varchar(2) DEFAULT NULL,
  `discount_applications` text,
  `payment_method` varchar(50) DEFAULT NULL
);

