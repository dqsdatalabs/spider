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
    name = 'pminternational_sahibinden.com'
    execution_type='testing'
    country='turkey'
    locale='tr'    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://pminternational.sahibinden.com/kiralik-daire?sorting=storeShowcase",
                "property_type" : "apartment"
            },
            {
                "url" : "https://pminternational.sahibinden.com/kiralik-residence?sorting=storeShowcase",
                "property_type" : "apartment"
            },
            {
                "url" : "https://pminternational.sahibinden.com/kiralik-mustakil-ev?sorting=storeShowcase",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//li[@class='mui-btn']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Pminternational_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title","//div[@class='classifiedDetailTitle']/h1/text()")

        external_id=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'İlan No')]//following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent="".join(response.xpath(
            "//div[@class='classifiedInfo ']/h3[contains(.,'TL')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Net')]//following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Oda')]//following-sibling::span/text()").get()
        if room_count:
            room_count=room_count.strip().split("+")
            item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
        
        address="".join(response.xpath("//div[@class='classifiedInfo ']/h2//a/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        desc="".join(response.xpath("//div[@id='classifiedDescription']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Bulunduğu')]//following-sibling::span/text()").get()
        if floor=='Kot 1':
            item_loader.add_value("floor", "-1")
        elif floor:
            item_loader.add_value("floor", floor.strip())
            
        balcony=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Balkon')]//following-sibling::span/text()[contains(.,'Var')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Eşyalı')]//following-sibling::span/text()[contains(.,'Evet')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        images=[x for x in response.xpath("//div[@class='classifiedDetailPhotos']/div/ul/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilties=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Aidat')]//following-sibling::span/text()[not(contains(.,'Belirtilmemiş'))]").get()
        if utilties:
            item_loader.add_value("utilities", True)
        
        deposit=response.xpath(
            "//div[@class='classifiedInfo ']/ul/li[contains(.,'Depozito')]//following-sibling::span/text()[not(contains(.,'Belirtilmemiş'))]").get()
        if deposit:
            item_loader.add_value("deposit", True)
        
        elevator=response.xpath("//li[@class='selected'][contains(.,'Asansör')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        terrace=response.xpath("//li[@class='selected'][contains(.,'Teras')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        
        washing_machine=response.xpath("//li[@class='selected'][contains(.,'Çamaşır Makinesi')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        
        dishwasher=response.xpath("//li[@class='selected'][contains(.,'Bulaşık Makinesi')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
            
        parking=response.xpath("//li[@class='selected'][contains(.,'Otopark')]/text()").get()
        garage=response.xpath("//li[@class='selected'][contains(.,'Garaj')]/text()").get()
        if parking or garage:
            item_loader.add_value("parking",True)
            
        swimming_pool=response.xpath("//li[@class='selected'][contains(.,'Yüzme Havuzu')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
        

        yield item_loader.load_item()
