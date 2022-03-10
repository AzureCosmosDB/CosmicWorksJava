package com.azure.cosmos.examples.models;

import java.util.List;

public class Models {

    public static class Product {

        public Product() {
        }
    
        public List<Tag> getTags() {
            return tags;
        }
        public String getId() {
            return id;
        }
        public String getCategoryId() {
            return categoryId;
        }
        public String getCategoryName() {
            return categoryName;
        }               
        public void setCategoryName(String categoryName) {
            this.categoryName = categoryName;
        }
        public String id;
        public String categoryId;
        public String categoryName;
        public String sku;
        public String name;
        public String description;
        public double price;
        public List<Tag> tags;
    }    

    public static class Tag
    {
        public String id;
        public String name;
    }

    public static class ProductCategory {

        public ProductCategory() {
        }
    
        public String getId() {
            return id;
        }
    
        public void setId(String id) {
            this.id = id;
        }
    
        public String getName() {
            return name;
        }
    
        public void setName(String name) {
            this.name = name;
        }
    
        public String getType() {
            return type;
        }
    
        public void setType(String type) {
            this.type = type;
        }    
    
        private String id;
        private String name;
        private String type;
    }

    public static class SalesOrder
    {
        public void setId(String id) {
            this.id = id;
        }
        public void setType(String type) {
            this.type = type;
        }
        public void setCustomerId(String customerId) {
            this.customerId = customerId;
        }
        public void setOrderDate(String orderDate) {
            this.orderDate = orderDate;
        }
        public void setShipDate(String shipDate) {
            this.shipDate = shipDate;
        } 
        public void setDetails(List<SalesOrderDetails> details) {
            this.details = details;
        }                                       

        public String id;
        public String type;
        public String customerId;
        public String orderDate;
        public String shipDate;
        public List<SalesOrderDetails> details;
    }

    public static class SalesOrderDetails
    {
        public void setSku(String sku) {
            this.sku = sku;
        } 
        public void setName(String name) {
            this.name = name;
        }
        public void setPrice(double price) {
            this.price = price;
        }
        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }                        
        public String sku;
        public String name;
        public double price;
        public int quantity;
    }

    public static class CustomerV1
    {
        public String getId() {
            return id;
        }
        public String id;
        public String title;
        public String firstName;
        public String lastName;
        public String emailAddress;
        public String phoneNumber;
        public String creationDate;
    }    

    public static class CustomerV2
    {
        public String getId() {
            return id;
        }
        public String id;
        public String title;
        public String firstName;
        public String lastName;
        public String emailAddress;
        public String phoneNumber;
        public String creationDate;
        public List<CustomerAddress> addresses;
        public Password password;
    }
    public static class CustomerV4
    {
        public String getId() {
            return id;
        }
        public String id;
        public String type;
        public String customerId;
        public String title;
        public String firstName;
        public String lastName;
        public String emailAddress;
        public String phoneNumber;
        public String creationDate;
        public List<CustomerAddress> addresses;
        public Password password;
        public int salesOrderCount;
    }

    public static class CustomerAddress
    {
        public String addressLine1;
        public String addressLine2;
        public String city;
        public String state;
        public String country;
        public String zipCode;
        public Location location;
    }

    public static class Location
    {
        public String type;
        public List<Float> coordinates;
    }

    public static class Password
    {
        public String hash;
        public String salt;
    }
    
}


