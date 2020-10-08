package cn.enjoy.mall.vo;

import java.io.Serializable;

public class OrderCreateVo implements Serializable{
    private Long addressId;

    public Long getAddressId() {
        return addressId;
    }

    public void setAddressId(Long addressId) {
        this.addressId = addressId;
    }
}
