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
    name = 'cabinetmansart_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Cabinetmansart_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.cabinet-mansart.com/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=2&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=", "property_type": "house"},
            {"url": "http://www.cabinet-mansart.com/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_REPLACE=1&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_65_REPLACE=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']/article//a[@class='titreBien']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        property_type = response.meta.get('property_type')
        if response.xpath("//h1[contains(.,'studio') or contains(.,'STUDIO')]").get(): item_loader.add_value("property_type", 'studio')
        else: item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_source", "Cabinetmansart_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        
        price = response.xpath("//div[contains(@class,'prix loyer')]/span[contains(.,'€')]//text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace("\xa0","."))

        external_id = response.xpath("//div[contains(@class,'reference')]//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[1].strip())

        if response.xpath("//h1[contains(.,'parking') or contains(.,'PARKING')]").get(): item_loader.add_value("parking", True)
        if response.xpath("//span[@class='alur_location_meuble']/text()[contains(.,'non meublé')]").get(): item_loader.add_value("furnished", False)
        if response.xpath("//span[@class='alur_location_meuble']/text()[contains(.,'meublé')]").get(): item_loader.add_value("furnished", True)
        
        room_count = response.xpath("//div[contains(@class,'carac')]/div[contains(.,'Pièce')]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Pièce")[0].strip())
    
        square = response.xpath("//div[contains(@class,'carac')]/div[contains(.,'m²')]//text()").extract_first()
        if square:
            square_meters=square.split("m")[0]
            item_loader.add_value("square_meters",square_meters.strip() )
  
        deposit = response.xpath("//span[@class='alur_location_depot']/text()").extract_first()
        if deposit :
            deposit=deposit.split(":")[1].split("€")[0]
            item_loader.add_value("deposit",deposit.replace("\xa0",".").strip() )

        desc = "".join(response.xpath("//div[contains(@class,'description')]/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
       
        address = response.xpath("//span[@class='alur_location_ville']/text()").extract_first()
        if address:
            for x in address.split(" "):
                if x.isdigit():
                    zipcode=x
                    item_loader.add_value("zipcode", zipcode.strip())
            if zipcode:
                item_loader.add_value("address", address.strip(str(zipcode)))
                item_loader.add_value("city", address.strip(str(zipcode)))
            else:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)
    
        img = response.xpath("//div[@class='diapoDetail']/div/@style").extract()
        if img:
            images = []
            for x in img:
                image =  x.split("('")[1].split("')")[0]
                if image:
                    images.append(image)
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "CABINET MANSART")
        item_loader.add_value("landlord_phone", "01 30 83 16 16")
        item_loader.add_value("landlord_email", "cabinet@cabinet-mansart.com")
        yield item_loader.load_item()