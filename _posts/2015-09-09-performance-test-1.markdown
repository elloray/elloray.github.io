---
layout: post
title:  "性能测试套件的实现(1)"
date:   2015-09-10 00:23:25
categories: code
tags: java
image: /assets/article_images/2014-11-30-mediator_features/night-track.JPG
---
之前在做性能测试，优先选择的是jmeter+nmon的组合，但是用了一段时间，发现jmeter不太满足我做性能测试的需求，于是就自己设计了一套性能测试套件。

在构想这个性能测试套件的时候，我对性能测试的需求做了排序:

1. 能压出符合要求的qps
2. 能精确反应业务性能指标
3. 适应rpc，http等多种场景的压力测试
4. 可以支持以稳定的qps运行 
5. 可以嗅探压力瓶颈

整个套件的设计以这个几个大需求作为基础，具体设计下节详解