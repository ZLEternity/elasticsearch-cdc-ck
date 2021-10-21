# elasticsearch-cdc-ck

抓取elasticsearch cdc(Change Data Capture)数据，批量写到clickhouse；

该插件支持ES6.8.5版本

## 使用

指定cdc数据写入到的clickhouse集群的域名、访问域名的用户名密码、cdc数据写到clickhouse集群的数据库名、批量写入clickhouse的批次大小和时间

PUT _cluster/settings
{
  "persistent": {
    "indices.cdc.ck.domain": "sys-elasticsearch-cdc-data-service.ck.qa.sys.k8s.cloud.qa.nt.xxx.com",
    "indices.cdc.ck.username": "ch_jdbc_ck100034224",
    "indices.cdc.ck.password": "xxxxxxx",
    "indices.cdc.ck.db": "elasticsearch_cdc",
    "indices.cdc.ck.batch.size": 1,
    "indices.cdc.ck.batch.time": 1000
  }
}


指定索引是否打开cdc记录功能，打开cdc功能后指定数据写入的表名

PUT zmccc
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.cdc.enabled": true,
    "index.cdc.ck.table": "elasticsearch_cdc_info"
  },
  "mappings": {
    "_doc":{
      "properties": {
      "content": {
        "type": "text"
      },
      "doc_id": {
        "type": "integer"
      },
      "name": {
        "type": "keyword"
      },
      "nested_test": {
        "type": "nested",
        "properties": {
          "user_id": {
            "type": "keyword"
          },
          "age": {
            "type": "integer"
          }
        }
      }
    }
    }
  }
}


测试数据：
POST zmccc/_doc/20
{
	"doc_id": 1,
	"content": "i am content",
	"name": "zmc",
	"nested_test": [{
		"user_id": "zmc_id",
		"age": 25
	}, {
		"user_id": "zmc_id2",
		"age": 25
	}]
}




## 插件开发调试简要视频教程

https://www.bilibili.com/video/BV1mP4y187Pn?spm_id_from=333.999.0.0




