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
    name = 'laugierfine_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Laugierfine_PySpider_france"
    start_urls = ["https://laugierfine.immo/location-immobilier"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='gridAnnonces']//div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)   
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "tour" in response.url:
            return 
        status = response.xpath("//h1[@class='fiche fw400']/text()").get()
        if status and "parking" in status.lower():
            return
     
        item_loader.add_value("external_id", response.url.split('-')[-1].strip())

        item_loader.add_value("external_link", response.url)
        property_type=response.xpath("//h1[@class='fiche fw400']/text()").get()
        if property_type: 
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type", "apartment")
            if "maison" in property_type.lower():
                item_loader.add_value("property_type","house")
            if "local" in property_type.lower():
                item_loader.add_value("property_type","room")
            if "bureaux" in property_type.lower():
                return 
            if "inconnu" in property_type.lower():
                return 
       
        item_loader.add_value("external_source", self.external_source)
        title =response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent =response.xpath("//div[@class='bgBlue5 white']/div/text()").get()
        if rent:     
           item_loader.add_value("rent", rent.split('€')[0].strip().replace(" ",""))  
        item_loader.add_value("currency","GBP") 
        charges=response.xpath("//div[@class='bgBlue4 white']/div/text()").get()
        if charges:
            item_loader.add_value("utilities",charges.replace("€","").strip())
        deposit=response.xpath("//div[@class='bgBlue3 white']/div/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split('.')[0])
        address = response.xpath("//h2[@class='fiche darkBlue']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(' ')[0]
            item_loader.add_value("city", city)
            zipcode = address.split(' ')[1].split(',')[0].strip()
            item_loader.add_value("zipcode", zipcode)
        floor =response.xpath("//div[@class='pictonumber'][contains(.,'étage')]/text()").get() 
        if floor:   
            item_loader.add_value("floor",floor.split("e")[0].strip())    

        room_count =response.xpath("//ul[@id='listPieces']/li[contains(.,'pièce')]/text()").get()   
        if room_count:   
            item_loader.add_value("room_count",room_count.split(" ")[0].strip())
        else:
            room_count =response.xpath("//div[@class='pictonumber'][contains(.,'chambres')]/text()").get() 
            if room_count:   
                item_loader.add_value("room_count",room_count.split(" ")[0].strip())

        bathroom_count = response.xpath("//div[@class='pictonumber'][contains(.,'salle de bain')]/text()").get() 
        if bathroom_count and not "0" in bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0].strip())
        square =response.xpath("//div[@class='pictonumber'][contains(.,'m²')]/text()").get()
        if square:
            square_meters =  square.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters))) 
  
        desc = response.xpath("//div[@id='descriptif']//div//p//text()").get()
        if desc:
            item_loader.add_value("description", desc.strip())

        contact =response.xpath("//p[@class='mt1 fw500']//span[@class='fs2']/text()").get()
        if contact:            
            item_loader.add_value("landlord_phone", contact.split("-")[1].strip())
            item_loader.add_value("landlord_name", contact.split(":")[0].strip()) 

        images = [response.urljoin(x.split(": url(")[-1].split(")")[0]) for x in response.xpath("//ul[@class='uk-slideshow-items h100 txtcenter ']//li//@style").getall()]
        if images:
                item_loader.add_value("images", images)
 
        yield item_loader.load_item()