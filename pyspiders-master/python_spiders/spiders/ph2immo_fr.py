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
 

class MySpider(Spider):
    name = 'ph2immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            
            {"url": "https://ph2immo.fr/advanced-search/?type=appartement&max-price=&status=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=", "property_type": "apartment"},
            {"url": "https://ph2immo.fr/advanced-search/?keyword=&status=&type=studio&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=", "property_type": "apartment"},
            {"url": "https://ph2immo.fr/advanced-search/?keyword=&status=&type=maison&bedrooms=&min-area=&max-price=&bathrooms=&max-area=&min-price=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='property-item table-list']//div[contains(@class,'phone')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title=response.xpath("//div[@class='table-cell']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Ph2immo_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        rent = response.xpath("//span[@class='item-price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[0].strip().replace(' ', ''))
            item_loader.add_value("currency", 'EUR')
        item_loader.add_xpath("external_id","//span/span[contains(.,'Référence')]/following-sibling::span/text()")
        item_loader.add_xpath("floor", "//div[@class='aivoni-details'][contains(.,'Etage')]/label/text()")
        item_loader.add_xpath("zipcode","//li[@class='detail-zip'][contains(.,'Code Postal')]/text()")
 
        item_loader.add_xpath("city", "//li[@class='detail-city']/text()")
        item_loader.add_xpath("address","//span/span[contains(.,'Lieu')]/following-sibling::span/text()")
        item_loader.add_xpath("room_count","//span/span[contains(.,'Chambres')]/following-sibling::span/text()")

        square = response.xpath("//span/span[contains(.,'Surface')]/following-sibling::span/text()").extract_first()
        if square:
            square =square.split("m")[0].strip()
            square_meters = math.ceil(float(square.strip()))
            item_loader.add_value("square_meters",square_meters )
        
        energy_label = response.xpath("//div[contains(@class,'DPEBOX')]/h5[contains(.,'DPE')]/text()").extract_first()
        if energy_label:
            energy=energy_label.split(":")[1].split("(")[0]
            item_loader.add_value("energy_label",energy.strip())
        
               
        desc = "".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "piscine" in desc:
                item_loader.add_value("swimming_pool", True)
            if "terrasse" in desc:
                item_loader.add_value("terrace", True)
            if "meublé" in desc:
                item_loader.add_value("furnished", True)
   

        images = [response.urljoin(x) for x in response.xpath("//div[@class='slider-thumbs']//img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0517261041")
        item_loader.add_value("landlord_name", "PH2 IMMOBILIER")
  
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        item_loader.add_value("landlord_email","agency@ph2immobilier.fr")
        yield item_loader.load_item()