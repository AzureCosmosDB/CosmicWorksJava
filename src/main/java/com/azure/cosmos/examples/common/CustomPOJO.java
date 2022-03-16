// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

public class CustomPOJO {
    private String id;
    private String pk;
    private String type;
    private String productCount;
    private String categoryName;

    public String getType() {
        return type;
    }

    public void setType(String type) { this.type = type; }

    public String getProductCount() { return productCount; }

    public void setProductCount(String productCount) { this.productCount = productCount; }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) { this.categoryName = categoryName; }

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
