# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re

class MySpider(Spider):
    name = 'cesmeemlak_com'
    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr'
    external_source='Cesmeemlak_PySpider_turkey_tr'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.cesmeemlak.com.tr/tr/emlak/konut-kiralik-daire/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.cesmeemlak.com.tr/tr/emlak/konut-kiralik-mustakil-ev/",
                "property_type" : "house"
            },
            {
                "url" : "https://www.cesmeemlak.com.tr/tr/emlak/konut-kiralik-residence/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.cesmeemlak.com.tr/tr/emlak/konut-kiralik-villa/",
                "property_type" : "house"
            },
            {
                "url" : "https://www.cesmeemlak.com.tr/tr/emlak/konut-kiralik-yazlik/",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//span[contains(@class,'soft') and contains(.,'Kiralık')]/../..//a/@href").extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@title='Sonraki sayfa']/@href").get()
        if next_page:
            yield Request(
                url=next_page,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        
        item_loader = ListingLoader(response=response)

        holiday = " ".join(response.xpath("//div[@class='title-divider']/h1/text()").extract())
        if "sezonluk" in holiday.lower():
            return

        room=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Oda Sayısı')]]/div[@class='info']/text()").extract_first()
        square_meters=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Toplam m²')]]/div[@class='info']/text()").extract_first()

        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_source", "Cesmeemlak_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        external_id=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Emlak No')]]/div[@class='info']/span/text()").extract_first()
        item_loader.add_value("external_id",external_id)

        address=" ".join(response.xpath("//div[contains(@class,'property-title-top')]//div[contains(@class,'subtitle left')][1]/span[@class='icon icon-map-pin']/following-sibling::span//text()").extract())
        if address:
            addr = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address",addr)

        if square_meters is not None:
                item_loader.add_value("square_meters",square_meters.strip("m²"))
                
        if room is not None:
            add=0
            room_array=room.split("+")
            for i in room_array:
                add += int(math.floor(float(i)))#3.1+1
            item_loader.add_value("room_count",str(add) )

        rent=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Emlak Fiyatı')]]/div[@class='info']/span/text()").extract_first()
        if rent is not None:
                item_loader.add_value("rent",rent.strip("₺"))

        images = [response.urljoin(x)for x in response.xpath("//div[@id='property-photos-imagelist']/div//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        item_loader.add_value("currency", "TRY")

        city=response.xpath("//div[contains(@class,'subtitle left')]//span[2]//text()").extract_first()
        if city:
            item_loader.add_value("city",city)
        
        bathroom_count=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Banyo')]]/div[@class='info']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        description="".join(response.xpath("//div[contains(@class,'property-content')][2]/div//text()[normalize-space()]").extract())
        item_loader.add_value("description",description)
        if "dubleks" in description.lower():
            item_loader.add_value("property_type","house")
        else:
            item_loader.add_value("property_type", response.meta.get("property_type"))
        
        floor=response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Bulunduğu Kat')]]/div[@class='info']/text()").extract_first()
        floor2 = response.xpath("//div[contains(@class,'information-property')]//li[./div[contains(.,'Kat Sayısı')]]/div[@class='info']/text()").extract_first()

        if floor is not None:
            if floor == "Villa Tipi":
                item_loader.add_value("floor", floor2)
            elif "Belirtilmemiş" not in floor:
                item_loader.add_value("floor",floor)
    
        terrace=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Teras')]]/div[@class='icon']//@value").extract_first()
        if terrace =="1":
                item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace",False)

        elevator=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Asansör')]]/div[@class='icon']//@value").extract_first()
        if elevator =="1" :
                item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)

        balcony=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Balkon')]]/div[@class='icon']//@value").extract_first()
        if balcony=="1":
                item_loader.add_value("balcony",True)
        else:
            item_loader.add_value("balcony",False)

        

        dishwasher=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Bulaşık Makinesi')]]/div[@class='icon']//@value").extract_first()
        if dishwasher=="1":
            item_loader.add_value("dishwasher",True)
        else:
            item_loader.add_value("dishwasher",True)

        washing_machine=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Çamaşır Makinesi')]]/div[@class='icon']//@value").extract_first()
        if washing_machine =="1" :
            item_loader.add_value("washing_machine",True)
        else:
            item_loader.add_value("washing_machine",False)
            
        furnished=response.xpath("//div[@class='title'][contains(.,'Mobilya')]/following-sibling::div/text()").extract_first()
        if furnished:
            if "Eşyalı" in furnished or "Mobilyalı" in furnished:
                item_loader.add_value("furnished", True)
            else: 
                item_loader.add_value("furnished", False)

        parking=response.xpath("//div[contains(@class,'information-feature')][./div[contains(.,'Otopark')]]/div[@class='icon']//@value").extract_first()
        if parking=="1":
                item_loader.add_value("parking",True)
        else:
            item_loader.add_value("parking",False)

        if "havuz" in response.url.lower() or "havuz" in description.lower():
            item_loader.add_value("swimming_pool", True)
        
        item_loader.add_value("landlord_phone", "0232 712 88 00")
        item_loader.add_value("landlord_email", "info@cesmeemlak.com.tr")
        item_loader.add_value("landlord_name", "Çeşme Atilla Emlak")
        item_loader.add_xpath("latitude", "//div[@id='map-display']/@lat")
        item_loader.add_xpath("longitude", "//div[@id='map-display']/@long")

        yield item_loader.load_item()