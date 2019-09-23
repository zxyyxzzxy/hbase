package com.zxy.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
// import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.Logger;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Repository;

@Repository
public class HbaseQueryDao {

	Logger logger = Logger.getLogger(HbaseQueryDao.class); 
	
	private String columnFamily = "columns";

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	@Autowired
	private HbaseTemplate hbaseTemplate;

	public <T> T queryForBeanByRowKey(String tableName, String rowKey, final Class<T> beanType) {
		
		logger.info("-----------------------HbaseQueryDao.queryForBeanByRowKey-----------------------------------");
		logger.info("tableName:" + tableName + ",rowKey:" + rowKey);
		logger.info("-----------------------HbaseQueryDao.queryForBeanByRowKey-----------------------------------");
        return hbaseTemplate.get(tableName, rowKey, new RowMapper<T>() {  
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
            	
            	Map<byte[], byte[]> map = result.getFamilyMap(Bytes.toBytes(columnFamily));
            	T t = beanType.newInstance();
            	BeanWrapper beanWrapper = new BeanWrapperImpl(t);
                for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
                	beanWrapper.setPropertyValue(Bytes.toString(entry.getKey()),Bytes.toString(entry.getValue()));
                }
//                List<Cell> ceList = result.listCells();
//                
//                if (ceList != null && ceList.size() > 0) {  
//                    for (Cell cell : ceList) {
//                    	
//                    	String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),  
//                                cell.getQualifierLength());
//                    	String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//                    	if (beanWrapper.isWritableProperty(columnName)) {
//                    		beanWrapper.setPropertyValue(columnName,columnValue);
//                    	}
//                    	
//                    }
//                }

                return t;  
            }  
        });  
	}
	
	public Map<String, String> queryForMapByRowKey(String tableName, String rowKey) {
		
		logger.info("-----------------------HbaseQueryDao.queryForMapByRowKey-----------------------------------");
		logger.info("tableName:" + tableName + ",rowKey:" + rowKey);
        return hbaseTemplate.get(tableName, rowKey, new RowMapper<Map<String, String>>() {  
            @Override
            public Map<String, String> mapRow(Result result, int rowNum) throws Exception {

            	/*Map<byte[], byte[]> mapColumn = result.getFamilyMap(Bytes.toBytes(columnFamily));
            	Map<String, String> map = new HashMap<String, String>();
                for(Map.Entry<byte[], byte[]> entry : mapColumn.entrySet()){
                	map.put(Bytes.toString(entry.getKey()),Bytes.toString(entry.getValue()));
                }*/
                
                Map<String, String> map = new HashMap<String, String>();
            	if(result.listCells()!=null && result.listCells().size()>0){  
            		Map<byte[], byte[]> mapColumn = result.getFamilyMap(Bytes.toBytes(columnFamily));
            		for(Map.Entry<byte[], byte[]> entry : mapColumn.entrySet()){
                    	map.put(Bytes.toString(entry.getKey()),Bytes.toString(entry.getValue()));
                    }
            	}
                return map;  
            }  
        });  
	}

	public List<Map<String, String>> queryForListByScanRange(String tableName, String startRow, String stopRow) {
		
		logger.info("-----------------------HbaseQueryDao.queryForListByScanRange-----------------------------------");
		logger.info("tableName:" + tableName + ",startRow:" + startRow+ ",stopRow:" + stopRow);
		
		Scan scan = new Scan();  
		if (startRow != null) {
			scan.setStartRow(Bytes.toBytes(startRow));
        } else {
        	scan.setStartRow(Bytes.toBytes(""));
        }
        if (stopRow != null) {
        	scan.setStopRow(Bytes.toBytes(stopRow));
        } else {
        	scan.setStopRow(Bytes.toBytes(""));
        }
        
        Filter pf = new PrefixFilter(Bytes.toBytes(startRow));
        scan.setFilter(pf);

        //scan.setBatch(batch)
        return hbaseTemplate.find(tableName, scan, new RowMapper<Map<String, String>>() {
			
            public Map<String, String> mapRow(Result result, int rowNum) throws Exception {  
            	
            	Map<String, String> map = new HashMap<String, String>();
            	String  rowkey = "";
            	if(result.listCells()!=null && result.listCells().size()>0){  
	            	for (Cell cell : result.listCells()) {  
	            		rowkey =Bytes.toString( cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());  
	            		String value =Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());  
	                    String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());  
	                    String quali = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());  
	                    if(columnFamily.equals(family)) {
	                    	map.put(quali,value);
	                    }
//	                    System.out.println(family+"_"+quali+"_"+ value);  
	                }
	            	map.put("rowkey",rowkey);
            	}
                /*
                 Map<byte[], byte[]> mapColumn = result.getFamilyMap(Bytes.toBytes(columnFamily));
                 for(Map.Entry<byte[], byte[]> entry : mapColumn.entrySet()){
                	map.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
                }*/
                return map;  
            }  
        });  
	}
	//用正则表达式模糊查询
	public List<Map<String, String>> queryForListByRegex(String tableName,String regex) {
		logger.info("-----------------------HbaseQueryDao.queryForListByRegex-----------------------------");
		logger.info("tableName:" + tableName + ",rowKey:" + regex);
		Scan scan = new Scan();  
		/*List<Filter> filters = new ArrayList<Filter>();   
		Filter filter1 = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("kpi")); 
		filters.add(filter1); 
		Filter filter2 = new RowFilter(CompareOp.EQUAL,new RegexStringComparator("kqi")); 
		filters.add(filter2); 
		FilterList filterList = new FilterList(filters); 
		scan.setFilter(filterList);*/
		Filter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex)); 
		scan.setFilter(filter);
        return hbaseTemplate.find(tableName, scan, new RowMapper<Map<String, String>>() {
			
            public Map<String, String> mapRow(Result result, int rowNum) throws Exception {  
            	
            	Map<String, String> map = new HashMap<String, String>();
            	String  rowkey = "";
            	if(result.listCells()!=null && result.listCells().size()>0){  
	            	for (Cell cell : result.listCells()) {  
	            		rowkey =Bytes.toString( cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());  
	            		String value =Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());  
	                    String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());  
	                    String quali = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());  
	                    if(columnFamily.equals(family)) {
	                    	map.put(quali,value);
	                    }
	                }
	            	map.put("rowkey",rowkey);
            	}
                return map;  
            }  
        });  
	}
	//查询除了regex以外的值
	public List<Map<String, String>> queryForListNotByRegex(String tableName,String regex) {
		logger.info("-----------------------HbaseQueryDao.queryForListByRegex-----------------------------");
		logger.info("tableName:" + tableName + ",rowKey:" + regex);
		Scan scan = new Scan();  
		Filter filter = new RowFilter(CompareOp.NOT_EQUAL,new RegexStringComparator(regex)); 
		scan.setFilter(filter);
        return hbaseTemplate.find(tableName, scan, new RowMapper<Map<String, String>>() {
			
            public Map<String, String> mapRow(Result result, int rowNum) throws Exception {  
            	
            	Map<String, String> map = new HashMap<String, String>();
            	String  rowkey = "";
            	if(result.listCells()!=null && result.listCells().size()>0){  
	            	for (Cell cell : result.listCells()) {  
	            		rowkey =Bytes.toString( cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());  
	            		String value =Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());  
	                    String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());  
	                    String quali = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());  
	                    if(columnFamily.equals(family)) {
	                    	map.put(quali,value);
	                    }
	                }
	            	map.put("rowkey",rowkey);
            	}
                return map;  
            }  
        });  
	}
	
	
	
	

	public void save(String tableName, String rowKey, Map<String,String> rowMap) {

		for (Entry<String, String> en : rowMap.entrySet()) {
			hbaseTemplate.put(tableName, rowKey, columnFamily, en.getKey(), Bytes.toBytes(en.getValue()));
		}
	}
	
	// public void delete(String tableName, final String rowKey) {
	//
	// 	  hbaseTemplate.execute(tableName, new TableCallback<Boolean>() {
	// 	        public Boolean doInTable(HTableInterface table) throws Throwable {
	// 	            boolean flag = false;
	// 	            try{
	// 	                List<Delete> list = new ArrayList<Delete>();
	// 	                Delete d1 = new Delete(rowKey.getBytes());
	// 	                list.add(d1);
	// 	            	table.delete(list);
	// 	             flag = true;
	// 	            }catch(Exception e){
	// 	                e.printStackTrace();
	// 	            }
	// 	            return flag;
	// 	        }
	// 	    });
	// }
	
}