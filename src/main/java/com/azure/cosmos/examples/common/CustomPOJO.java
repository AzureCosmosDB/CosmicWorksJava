// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

public class CustomPOJO {
    private String id;
    private String pk;
    private String type;
    private String ProductCount;
    private String categoryName;;

    public CustomPOJO() {

    }

    public String getType() {
        return type;
    }
    public String getProductCount() {
        return ProductCount;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public CustomPOJO(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPk() { return pk; }

    public void setPk(String pk) {
        this.pk = pk;
    }

}
