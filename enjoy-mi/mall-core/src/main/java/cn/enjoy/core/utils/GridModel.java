/**
 * Created on 2015年9月3日 by Caiming
 */
package cn.enjoy.core.utils;

import com.github.pagehelper.PageInfo;

import java.io.Serializable;
import java.util.List;


public class GridModel<T> implements Serializable {

    /**
     * Comment for <code>serialVersionUID</code>
     */
    private static final long serialVersionUID = 7252085778810679755L;

    private Integer page;
    //总记录数
    private Long records;
    //页的大小
    private Integer total;

    private List<T> rows;

    public GridModel() {

    }

    /*	public GridModel(PageList<T> pageList) {
            this.page = pageList.getPaginator().getPage();
            this.records = pageList.getPaginator().getTotalCount();
            this.total = pageList.getPaginator().getTotalPages();
            this.rows = pageList;
        }*/
    public GridModel(List<T> list) {
        PageInfo<T> pageInfo = new PageInfo<>(list);
        this.page = pageInfo.getPageNum();
        this.records = pageInfo.getTotal();
        this.total = pageInfo.getPageSize();
        this.rows = pageInfo.getList();
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Long getRecords() {
        return records;
    }

    public void setRecords(Long records) {
        this.records = records;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<T> getRows() {
        return rows;
    }

    public void setRows(List<T> rows) {
        this.rows = rows;
    }

}
