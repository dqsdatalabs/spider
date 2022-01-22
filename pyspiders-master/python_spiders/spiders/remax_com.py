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
    name = 'remax_com' 
    execution_type='testing'
    country='turkey'
    locale='tr'  
    external_source="Remax_Com_PySpider_turkey_tr"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.remax.com.tr/konut/kiralik",
                    
                ],
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[@class='info']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,)
            seen = True
        if page == 2 or seen:
            url = response.xpath("//a[@class='next']/@href").get()
            yield Request(response.urljoin(url),callback = self.parse,meta={"page": page+1})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//title//text()").get()
        sale = "".join(response.xpath("//div[@class='portfoySlogan']/h1/text()").getall())
        if "satılık" in sale.lower():
            return
        if "satilik" in sale.lower():
            return
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        
        prop_type = response.xpath("//strong[.='Emlak Tipi']/following-sibling::span/text()").get()
        if prop_type and "Daire"==prop_type:
            item_loader.add_value("property_type", "apartment")

        room_count = "".join(response.xpath("//strong[.='Oda Sayısı']/following-sibling::span/text()").extract()) 
        if room_count:
            if "+" in  room_count:
                room_count1=room_count.split("+")
                room3=room_count1[0]+room_count1[1]
                item_loader.add_value("room_count", room3)
            else:
                item_loader.add_value("room_count", room_count)
        price = "".join(response.xpath("//div[@class='price-container']/strong/text()").extract())        
        if price:
            item_loader.add_value("rent", price.strip().replace(".",""))
        item_loader.add_value("currency", "TRY")
        bathroom_count=response.xpath("//strong[.='Banyo Sayısı']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", int(float(bathroom_count)))
        utilities = response.xpath("//strong[.='Aidat (TL)']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("TL",""))
        images=[x for x in response.xpath("//picture//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)

        address = "".join(response.xpath("//div[@class='breadcrumbs']/text()").extract())
        if address:
            item_loader.add_value("address", address.strip().replace("/",""))
            item_loader.add_value("city", address.split("/")[0].split("-")[0])

        external_id = response.xpath("//small[contains(.,'Portföy No')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])

        desc = "".join(response.xpath("//div[@class='content']//p//text()").extract())
        if desc:
            item_loader.add_value("description",desc)


        dishwasher=response.xpath("//span[contains(.,'Bulasık Makinesi')]/@class").get()
        if dishwasher and "active"==dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing=response.xpath("//span[contains(.,'Çamasır Makinesi')]/@class").get()
        if washing and "active"==washing:
            item_loader.add_value("washing_machine", True)

        balcony = response.xpath("//span[contains(.,'Balkon')]/@class").get()
        if balcony and "active"==balcony:
            item_loader.add_value("balcony", True)

        terrace =response.xpath("//span[contains(.,'Bahçe')]/@class").get()
        if terrace and "active"==terrace:
            item_loader.add_value("terrace", True)

        parking =response.xpath("//span[contains(.,'Otopark')]/@class").get()
        if parking and "active"==parking:
            item_loader.add_value("parking", True)

        elevator =response.xpath("//span[contains(.,'Asansör')]/@class").get()
        if elevator and "active"==elevator:
            item_loader.add_value("elevator", True)

        furnished=response.xpath("//strong[.='Eşyalı']/following-sibling::span/text()").get()
        if furnished and "Hayır"==furnished:
            item_loader.add_value("furnished", True)

        # item_loader.add_xpath("landlord_phone", "//div[contains(@class,'phone')][1]/a/text()")
        # item_loader.add_xpath("landlord_name", "//div[@class='font_16_Light'][1]/a/text()")
        yield item_loader.load_item()



    