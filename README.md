### 在原项目的基础上修改内容：
1. frontend/src/api/videos.js修改const API_BASE = 'http://100.0.0.5:31471'
2. cfg/configs.json修改"SavePath": "./MissAV"、"Proxy": ""
3. requirements.txt添加后三个依赖
4. 修改dockerfile

5. 添加文件：entrypoint.sh、webui目录及里面的文件

### 使用Docker下載
1. Build Docker (初次使用才需要)
```bash
git clone https://github.com/optisw/NassAV.git
cd NassAV

docker build -t nassav:latest .
docker run -d \
  --name nassav \
  --restart always \
  -p 8008:8008 \
  -v /root/Temp:/NASSAV/MissAV \
  nassav:latest
```

2. 下載
```bash
docker run --rm -v "/root/Temp:/NASSAV/MissAV" nassav 车牌号
```
