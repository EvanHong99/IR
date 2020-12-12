# TODO:
#  将数据库中的数据导出为文件（或者修改源程序使支持数据库内数据建倒排索引），直接用这个函数就可以构建好倒排索引
#  计算PageRank（networkx）并保存，计算文档向量（gensim）并保存
#  根据查询，返回url，根据url，获取PageRank、文档向量，进行相乘，评分，返回用户

**为 12 月 13 日（周日）**

大家每周日发送邮件简单总结一下本周完成了哪些内容，最后在截止日期前将代码、文档、演示视频打包（命名“学号
_ 姓名 _hw6”）发送到nkulixuy@163.com。

本次作业的要求是针对南开校内网站构建一个 Web 搜索引擎，为用户提供南开信息的查询服
务乃至个性化推荐。本次作业可以借助各种工具和包，希望大家善于利用以减少工作量。
1 具体实现
实现这次作业主要有网页抓取、文本索引、链接分析、查询服务、个性化查询几个步骤，个性
化推荐为扩展内容。
1.1 网页抓取
对南开大学各网页内容进行抓取。
1.2 文本索引
对网页及其锚文本构建索引，可以按锚文本、网页标题、URL 等域构建索引。
1.3 链接分析
使用 PageRank 对链接进行分析，评估网页权重
1.4 查询服务
使用向量空间模型并结合链接分析对查询结果进行排序，为用户提供站内查询、文档查询、短
语查询、通配查询、查询日志、网页快照等高级搜索功能。更多的内容可以参考百度（图 1) 或谷歌
（图 2）的高级搜索功能。
1.5 个性化查询
个性化查询为不同的用户提供不同的内容排序。可以实现一个账号登录系统，通过用户完善的
学院专业等个人信息为其呈现不同的查询结果；或者是记录用户的查询历史，通过历史查询来提供
个性化的查询结果。在 google 的查询中就会通过这些手段来优化用户的查询体验
1.6 个性化推荐
本次作业的扩展内容为个性化推荐，个性化推荐系统通过用户的个人信息和查询历史获取用户
可能的兴趣点，在用户查询时给用户推荐相关领域的其他内容。比如在百度上搜索 iphone，其会在
查询结果的右侧为你推荐 ipad、iMac 等相关产品（图 4）。

sdu视点新闻全站爬虫爬取+索引构建+搜索引擎查询练习程序

爬虫功能使用Python的scrapy库实现，并用MongoDB数据库进行存储。

索引构建和搜索功能用Python的Whoosh和jieba库实现。（由于lucene是java库，所以pyLucene库的安装极其麻烦，因此选用Python原生库Whoosh实现，并使用jieba进行中文分词。）

搜索网页界面用django实现，页面模板套用BootCDN。

1 要求

以下是检索的基本要求：可以利用lucene、nutch等开源工具，利用Python、Java等编程语言，但需要分别演示并说明原理。

1.1 Web网页信息抽取

以山东大学新闻网为起点进行网页的循环爬取，保持爬虫在view.sdu.edu.cn之内（即只爬取这个站点的网页），爬取的网页数量越多越好。

1.2 索引构建

对上一步爬取到的网页进行结构化预处理，包括基于模板的信息抽取、分字段解析、分词、构建索引等。

1.3 检索排序

对上一步构建的索引库进行查询，对于给定的查询，给出检索结果，明白排序的原理及方法。




# EverythinNKU

## web crawler

1. 获取nk vpn首页urls，作为种子url
2. 根据种子url，爬取网页，将该url加入used url列表，
    将网页中的链接加入到unused url列表，防止重复爬取。
3. 将网页中重要的信息（一般包含在a标签下）保存为文件供建立索引

文件格式
filename=base url.html


**做的优化**
1. 多线程
2. 锁机制
3. 日志（还没完成）
```
sql select
3.439732074737549
if in
0.006981372833251953
```


## index

## 难点
暗网/动态资源爬取：一般的爬虫无法爬取搜索引擎中的信息

**重复url**

使用
```mysql
 group by unused
```

**内存爆炸**

主要原因是unused url太多，但是不清楚是因为什么导致之前的去重有问题，后来采用group by来去重

**对Python语言的掌握度不够，发生错误但是一直没有发现**

```python
uniq_urls = [i for i in set(new_urls) if i not in unused_url and used_url]
```

该语句并不能将i排除在两个列表之外，而是只能将i排除在第一个列表之外。

此bug可能是导致我数据库内容大量重复的原因之一。

修改为

```python
uniq_urls = [i for i in set(new_urls) if i not in unused_url and i not in used_url]
```

**避免死锁**

使用`threading.Rlock`可重用锁

**根url下的直接链接地址**

```html
<div class="text-muted description">
    <p class="text"><a href='/2020/1114/c19665a317736/page.htm' target='_blank' title='金融学院召开课程思政建设工作推动会'>（通讯员：徐静）为进一步贯彻落实《南开大学课程思政建设实施方案》和南开大学课...</a></p>
</div>
```
```<!--
# delete from _unused_url where unused not like '%nankai%';
# select *
# from _used_url
# where used='http://www.mathlib.nankai.edu.cn/';


delete
from links;
delete
from unused_url;
delete
from used_url;

delete from used_url
where used='https://www.nankai.edu.cn/';
delete from used_url
where used in (
    select `linked url`
    from links
    where `base url`='https://www.nankai.edu.cn/'
    );

delete from used_url
where used in ('https://www.nankai.edu.cn/',
'http://xxgk.nankai.edu.cn/',
'https://www.nankai.edu.cn/',
'http://www.lib.nankai.edu.cn/',
'https://www.nankai.edu.cn/',
'https://nankai.edu.cn',
'https://www.nankai.edu.cn/',
'http://xb.nankai.edu.cn',
'https://www.nankai.edu.cn/',
'http://en.nankai.edu.cn/',
'https://www.nankai.edu.cn/',
'http://news.nankai.edu.cn/',
'https://www.nankai.edu.cn/',
'http://weekly.nankai.edu.cn/');


select count(*)
from _unused_url
group by unused;
select *
from _used_url
group by used;

insert into unused_url(unused) values (
'http://xb.nankai.edu.cn',
'http://cwc.nankai.edu.cn',
'http://www.lib.nankai.edu.cn',
'http://jwc.nankai.edu.cn',
'http://jsfz.nankai.edu.cn',
'http://rsc.nankai.edu.cn',
'http://xgb.nankai.edu.cn',
'http://graduate.nankai.edu.cn',
'http://ygb.nankai.edu.cn',
'http://nkoa-webvpn.nankai.edu.cn',
'http://i.nankai.edu.cn',
'http://urp.nankai.edu.cn',
'http://online.nankai.edu.cn',
'http://zhgl.nankai.edu.cn',
'http://sheke.nankai.edu.cn',
'http://less.nankai.edu.cn',
'http://eamis.nankai.edu.cn',
'http://yjzj.nankai.edu.cn',
'http://xsfww.nankai.edu.cn',
'http://sysaqk.nankai.edu.cn',
'http://reg.nankai.edu.cn',
'http://wlaq.nankai.edu.cn',
'http://print.lib.nankai.edu.cn',
'http://paper.lib.nankai.edu.cn',
'http://ic.lib.nankai.edu.cn',
'http://sso.visiting.nankai.edu.cn/ssojinxiu',
'http://zyfw.nankai.edu.cn',
'http://shsj.nankai.edu.cn',
'http://kwhd.nankai.edu.cn',
'http://jw.nankai.edu.cn',
'http://techshow.nankai.edu.cn',
'http://elearning.nankai.edu.cn',
'http://sbc.nankai.edu.cn',
'http://zbb.nankai.edu.cn/sfw_ks/caslogin.jsp',
'http://czfw.nankai.edu.cn',
'http://cwc.nankai.edu.cn',
'http://xcb.nankai.edu.cn',
'http://nktw.nankai.edu.cn',
'http://tzb.nankai.edu.cn',
'http://sd.nankai.edu.cn',
'http://jnjt.nankai.edu.cn',
'http://ltxc.nankai.edu.cn',
'http://youeryuan.nankai.edu.cn',
'http://archives.nankai.edu.cn',
'http://it.nankai.edu.cn',
'http://finance.nankai.edu.cn',
'http://edp.nankai.edu.cn',
'http://it.nankai.edu.cn',
'http://bylw.nankai.edu.cn',
'http://oa.tas.nankai.edu.cn',
'http://www.tianjinforum.nankai.edu.cn',
'http://mem.nankai.edu.cn',
'http://mbajx.nankai.edu.cn',
'http://mbaxw.nankai.edu.cn',
'http://vr.nankai.edu.cn',
'http://www.mathlib.nankai.edu.cn',
'http://swsyzx.nankai.edu.cn',
'http://suguan.nankai.edu.cn',
'http://nyjfcb.nankai.edu.cn',
'http://nkuefnew.nankai.edu.cn',
'http://nyhd.nankai.edu.cn',
'http://yqjj.nankai.edu.cn',
'http://bksj.nankai.edu.cn',
'http://wenchuang.nankai.edu.cn',
'http://tyxx.nankai.edu.cn/tyexam',
'http://www.lib.nankai.edu.cn',
'http://weekly.nankai.edu.cn',
'http://ca.nankai.edu.cn',
'http://soft.nankai.edu.cn');
insert into unused_url(unused) values ('http://less.nankai.edu.cn/equipment?tag=%E5%85%89%E8%B0%B1%E4%BB%AA%E5%99%A8')

select count(*) from used_url where used regexp '.*finance.*';
select count(*) from links where `base url` regexp '.*finance.*';
-->```