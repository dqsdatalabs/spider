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
    name = 'sixteen_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Sixteen_immo_PySpider_france_fr"
    def start_requests(self):
        start_urls = [
            {"url": "https://sixteen-immo.fr/fr/locations", "property_type": "apartment"},
	        
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[@class='_list listing']/li/a"):
            url = response.urljoin(item.xpath("./@href").get())
            title = item.xpath(".//h2/text()").get()
            property_type = ""
            if "appartement" in title.lower():
                property_type = "apartment"
            elif "maison" in title.lower():
                property_type = "house"
            if property_type:
                yield Request(url, callback=self.populate_item, meta={'property_type': property_type})
        next_url = response.xpath("//li[@class='next']/a/@href").get()
        if next_url:
            yield Request(response.urljoin(url), callback=self.parse)
       

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//li[text()='Référence ']/span/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        price = response.xpath("//div[@class='zone zone-top-content ']//div[@class='info']//li[contains(text(),'€')]/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
     
        deposit = response.xpath("//li[contains(text(),'Dépôt de garantie')]/span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.split(",")[0].replace(" ","").replace("\u202f", ""))
        utilities = response.xpath("//li[contains(text(),'Provision sur charges')]/span/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0].replace(" ",""))

        square = response.xpath("//li[contains(text(),'Surface')]/span/text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.replace("m²","").strip()))
            item_loader.add_value("square_meters",square_meters )

        room = response.xpath("//li[text()='Pièces ']/span/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip().split(" ")[0])
        floor = response.xpath("//li[text()='Étage ']/span/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.strip())
        item_loader.add_xpath("title", "//h1/text()")
           
        desc = "".join(response.xpath("//p[@id='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        # terrace = response.xpath("//div[@class='estate-detail'][contains(.,'Terrasse')]//text()").extract_first()
        # if terrace:
        #     item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(text(),'Parking')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[text()='Meublé']/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
       
        elevator = response.xpath("//li[contains(text(),'Ascenseur')]/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
 
        address=",".join(response.xpath("//div[@class='zone zone-top-content ']//div[@class='info']/h2//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
      
        images = [x for x in response.xpath("//div[@class='slider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        # bathroom=response.xpath("//li[contains(text(),'Salle')]/span/text()").get()
        # if bathroom:
        #     item_loader.add_value("bathroom_count", bathroom.strip())
        
        
        item_loader.add_value("landlord_phone", "06 14 03 77 72")
        item_loader.add_value("landlord_email", "gerance@admine.fr")
        item_loader.add_value("landlord_name", "Shirine TABET KHOURY")
        yield item_loader.load_item()