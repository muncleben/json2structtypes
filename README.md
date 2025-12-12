# json2structtypes
Get PySpark StructTypes from sample JSON file.

## 使用方法

1. 安装依赖  
   ```bash
   pip install -r requirements.txt
   ```

2. 准备一份示例 JSON 文件，例如:在sample_data文件夹下的`device.json`，内容如下：  
   ```json
   {
        "device_id": "iot_001",
        "name": "智能温湿度传感器",
        "online": true,
        "battery": 87,
        "temperature": 23.6,
        "humidity": 45.2,
        "location": {
            "latitude": 31.2304,
            "longitude": 121.4737,
            "address": "上海市浦东新区"
        },
        "sensors": [
            {
            "type": "temperature",
            "unit": "celsius",
            "value": 23.6
            },
            "sensors": [
                {
                    "type": "temperature",
                    "unit": "celsius",
                    "value": 23.6
                },
                {
                    "type": "humidity",
                    "unit": "percent",
                    "value": 45.2
                }
            ],  
        "tags": ["indoor", "office", "floor3"]
    }
   ```

3. 运行脚本之前，修改对应脚本中的 example_path 为你的 JSON 文件路径，例如：  
   ```python
   example_path = "sample_data/device.json"
   ```

4. 控制台将输出可直接复制到 PySpark 代码中的 StructType 定义，例如：  
   ```text
    Here is the inferred schema from json file: sample_data/device.json

    StructType([StructField('device_id', StringType(), False), StructField('name', StringType(), False), StructField('online', BooleanType(), False), StructField('battery', LongType(), False), StructField('temperature', DoubleType(), False), StructField('humidity', DoubleType(), False), StructField('location', StructType([StructField('latitude', DoubleType(), False), StructField('longitude', DoubleType(), False), StructField('address', StringType(), False)]), False), StructField('sensors', ArrayType(StructType([StructField('type', StringType(), False), StructField('unit', StringType(), False), StructField('value', DoubleType(), False)]), False), False), StructField('tags', ArrayType(StringType(), False), False)])
```

5. 将生成的 schema 直接用于 `df = df.withColumn("struct_column", from_json(col("json_column"), schema))` 即可。
