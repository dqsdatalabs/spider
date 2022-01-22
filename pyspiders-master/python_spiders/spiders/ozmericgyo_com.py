# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'ozmericgyo_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source = "Ozmericgyo_PySpider_turkey_tr"
    custom_settings = {
        "PROXY_ON": True,
        #"PROXY_PR_ON": True,
        "PASSWORD": "wmkpu9fkfzyo ",
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.ozmericgyo.com/kiralik-daireler/eRNA3xMId--o6NjOxkWPUd6fVSvxs6C2phldnMsyJuCjoXd1J6ZaiK0IYwmzUmDRDi0BOwmTJYU=&new=1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.ozmericgyo.com/kiralik-daireler/eRNA3xMId--o6NjOxkWPUd6fVSvxs6C2phldnMsyJuCqIrX7LRVp5uSjbYwBnjU-b0IzTO-alPY=&new=1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.ozmericgyo.com/kiralik-daireler/eRNA3xMId--o6NjOxkWPUd6fVSvxs6C2phldnMsyJuBupPMBbTJoUSNvgA5HTdeeCiumRAR6YXI=&new=1",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
     
        for item in response.xpath("//div[@id='content_uppList']/div[@class='item']/a[@class='title']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[@class='pagerNext']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='title']/h1/span/text()").get()
        item_loader.add_value("title",title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        external_id = "".join(response.xpath("//dl[@class='realty-details']/dd[1]/text()").extract())
        if external_id:
            item_loader.add_xpath("external_id", external_id.strip())
        
        room=response.xpath("//dl/div/dt[span[.='Oda Sayısı']]//following-sibling::dd/text()").extract_first().split()
        item_loader.add_value("room_count", str(int(room[0])+ int(room[2])))
        
        city=response.xpath("//h2/span[3]/text()").extract_first()
        district=response.xpath("//h2/span[4]/text()").extract_first()
        
        item_loader.add_value("address", city+" "+district)
        item_loader.add_value("city", city)
        
        square_meters=response.xpath("//dl/div/dt[span[.='Metrekare']]//following-sibling::dd/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split())
            
        rent=response.xpath("//div[@class='title']/h3/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("TL")[0].strip())
            item_loader.add_value("currency", "TRY")
        
        utilities=response.xpath("//dl/div/dt[span[.='Aidat']]//following-sibling::dd/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("TL")[0].strip())
        deposit=response.xpath("//dl/div/dt[span[.='Depozito']]//following-sibling::dd/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("TL")[0].strip())
        
        floor=response.xpath("//dl/div/dt[span[.='Bulunduğu Kat']]//following-sibling::dd/text()[not(contains(.,'Giriş')) and not(contains(.,'Bahçe Katı'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split())
            
        desc="".join(response.xpath("//span[@id='content_ctlRealtyDescription1_lblRealtyDescription']//text()").extract())
        if desc:
            item_loader.add_value("description", desc)
        
        
        balcony=response.xpath("//div[@class='c']/ul/li/span[.='Balkon']/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator=response.xpath("//div[@class='c']/ul/li/span[.='Asansör']/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished=response.xpath("//div[@class='c']/ul/li/span[.='Mobilyalı']/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
            
        wash=response.xpath("//div[@class='c']/ul/li/span[.='Beyaz Eşyalı']/text()").extract_first()
        if wash:
            item_loader.add_value("washing_machine", True)
            item_loader.add_value("dishwasher", True)
            
        parking=response.xpath("//div[@class='c']/ul/li/span[contains(.,'Otopark')]/text()").extract()
        if parking:
            item_loader.add_value("parking", True)
        
        swimming_pool=response.xpath("//div[@class='c']/ul/li/span[contains(.,'Yüzme Havuzu')]/text()").extract()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        terrace=response.xpath("//div[@class='c']/ul/li/span[contains(.,'Teras')]/text()").extract()
        terrace_=response.xpath("//div[@class='title']/h1/span[contains(.,'TERAS')]/text()").extract()
        if terrace:
            item_loader.add_value("terrace", True)
        elif terrace_:
            item_loader.add_value("terrace", True)
        
        name=response.xpath("//div[@class='c']/span[@class='contact-name']/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
            
        phone=response.xpath("//div[@class='c']/ul/li[2]/span[2]/text()").extract_first()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        
        images=[x for x in response.xpath("//div[@class='photo-gallery']/div/ul/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count", str(len(images)))
            
        
        yield item_loader.load_item()