导出豆瓣个人数据为 Excel 文件的线上服务

Live version: [http://wil.dog/douban](http://wil.dog/douban)

See this post for details: [http://wil.dog/2016/10/26/the-make-of-douban-exporter/](http://wil.dog/2016/10/26/the-make-of-douban-exporter/)


## Requirements
### Python 2.7 (Mac/Linux only, not work on Windows)

## How to Use
1. Run `pip install -r requirements.txt`
2. Run `python exporter.py`
3. Open index.html in the browser

## Extra
1. You can export your own cookies in LWP Set-Cookie3 format and copy them into `cookies.txt` file to export your private data, but also taking risk of getting your account temporarily banned due to suspicious activities.
2. You can change AVG_DELAY to a bigger one to avoid getting banned too easily.
