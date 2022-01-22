# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest 
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'km_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Km_Immo_PySpider_france'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):
        start_urls = [
            {"url": "http://www.kmimmo.com/a-louer/appartements/1", "property_type": "apartment"},
            {"url": "http://www.kmimmo.com/a-louer/maisons-villas/1", "property_type": "house"},
        ]  # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='btn btn-listing']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("(//ul[@class='pagination']/li[@class='active'])[1]/following-sibling::li/a/@href").get()
        if next_page:
            base_url = "http://www.kmimmo.com"
            next_page =  base_url + next_page
            yield Request(next_page, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1[@itemprop='name']/text()").get()
        if title:
            item_loader.add_value("title", title)
          
        external_id = response.xpath("//li[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(" ")[1])

        rent = "".join(response.xpath("//li/text()[contains(.,'€')]").getall())
        if rent:
            rent = rent.replace(" ","").replace("€","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("//span[@class='termInfos'][contains(.,'Surface')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("m²","").split(",")[0].strip())
        
        room_count=response.xpath("//span[@class='termInfos'][contains(.,'Nombre de pièce')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count=response.xpath("//span[@class='termInfos'][contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())   
        
        deposit = response.xpath("//span[@class='termInfos'][contains(.,'garantie')]/following-sibling::span/text()").get()
        if deposit and "non" not in deposit.lower():
            item_loader.add_value("deposit", deposit.replace("€","").strip().split(",")[0])

        utilities = response.xpath("//span[@class='termInfos'][contains(.,'Honoraires')]/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").strip().split(",")[0])
            
        city = response.xpath("//ol[@class='breadcrumb']/li[2]/a/text()").get()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address", city)
        
        zipcode = response.xpath("//span[@class='termInfos'][contains(.,'postal')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        furnished = response.xpath("//span[@class='termInfos'][contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished and "non" not in furnished.lower():
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//span[@class='termInfos'][contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace and "non" not in terrace.lower():
            item_loader.add_value("terrace", True)

        desc="".join(response.xpath("//p[@itemprop='description']/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description)
            
        images=[x for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "KM Immo")
        item_loader.add_value("landlord_phone", "06 94 43 88 22")
        item_loader.add_value("landlord_phone", "immobilier973@gmail.com")
        
        yield item_loader.load_item()