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
    name = 'cabinetlebreton_com'
    execution_type='testing'                          
    country='france'
    locale='fr'
    external_source='Cabinetlebreton_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.cabinetlebreton.com/nos-biens/location/?fwp_type=appartement%2Cappartement-t1-2%2Cappartement-t2", "property_type": "apartment"},
            {"url": "https://www.cabinetlebreton.com/nos-biens/location/?fwp_type=appartement-t3", "property_type": "apartment"},
            {"url": "https://www.cabinetlebreton.com/nos-biens/location/?fwp_type=maison-t3", "property_type": "house"},
            {"url": "https://www.cabinetlebreton.com/nos-biens/location/?fwp_type=studio", "property_type": "studio"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='listing-properties']/div[@class='block-listing']"):
            follow_url = item.xpath("./a/@href").extract_first()
            address = "".join(item.xpath("./a//div[@class='localisation']/text()").extract()).strip()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),'address':address})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("address", response.meta.get('address'))
        
        zipcode = response.meta.get('address')
        if zipcode:
            zipcode = zipcode.split(" ")
            if zipcode[0].isdigit():
                item_loader.add_value("zipcode", zipcode[0])
                item_loader.add_value("city", zipcode[1])
            elif zipcode[1].isdigit():
                item_loader.add_value("zipcode", zipcode[1])
                item_loader.add_value("city", zipcode[0])
                
        rent="".join(response.xpath("//div[@class='block-title']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ", ""))
        
        square_meters=response.xpath("//div[@class='property-options']/div[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.split('m²')[0].strip().replace(",",".")
            square_meters = math.ceil(float(square_meters))
            item_loader.add_value("square_meters", str(square_meters))
        elif response.xpath("//h1/text()[contains(.,'m2')]").get(): item_loader.add_value("square_meters", response.xpath("//h1/text()[contains(.,'m2')]").get().split('m2')[0].split('–')[-1].strip())
        
        room_count=response.xpath("//div[@class='property-options']/div[contains(.,'pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        
        bathroom_count = response.xpath("//div[@class='property-options']/div[contains(.,'salle')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0])
        
        external_id=response.xpath("//div/div/div[contains(.,'Référence')]").get()
        if external_id and "rc" not in external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())

        desc="".join(response.xpath("//div[contains(@class,'content-text')]/p//text()").getall())
        if desc:
            title = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
        images=[x for x in response.xpath("//div[contains(@class,'owl-carousel')]/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","CABINET LEBRETON")
        item_loader.add_xpath("landlord_phone","//div[contains(@class,'block-phone')]/text()")
        
        utilties=response.xpath("//div/div/div[contains(.,'Charges')]").get()
        if utilties:
            item_loader.add_value("utilities", int(float(utilties.split(':')[1].split('€')[0].strip())))
        
        deposit=response.xpath("//div/div/div[contains(.,'garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split(':')[1].split('€')[0].strip())))

        yield item_loader.load_item()