package mqz;

/**
 * @author maqingze
 * @version v1.0
 * @date 2019/3/8 10:27
 */
public class JavaOrder {

    private Long id;
    private String product;
    private Integer amount;

    public JavaOrder(Long id, String product, Integer amount) {
        this.id = id;
        this.product = product;
        this.amount = amount;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }
}
