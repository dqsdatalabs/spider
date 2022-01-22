# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin


class MySpider(Spider):
    name = 'speedimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "type" : 2,
                "property_type" : "house"
            },
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))

            payload = {
                "nature": "2",
                "type[]": r_type,
                "price": "",
                "age": "",
                "tenant_min": "",
                "tenant_max": "",
                "rent_type": "",
                "currency": "EUR",
                "homepage": "",
            }
            
            yield FormRequest(url="https://www.speedimmo.fr/fr/recherche/",
                            callback=self.parse,
                            formdata=payload,
                            meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//p[@class='price']/../@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))  
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Speedimmo_PySpider_" + self.country + "_" + self.locale)
        
        external_id = response.xpath("//span[@class='reference']//text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref.")[1].strip())

        item_loader.add_xpath("title", "//div[@class='title']/h1/text()")

        price = response.xpath("//div[@class='title']/h2/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ","."))
  
        room_count = response.xpath("//ul/li[contains(.,'Pièce')]/span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0].strip() )

        bathroom_count = response.xpath("//li[contains(.,'de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", "".join(filter(str.isnumeric, bathroom_count.strip())))
       
        square = response.xpath("//ul/li[contains(.,'Surface')]/span/text()").extract_first()
        if square:
            square_meters =square.split("m")[0].strip()
            item_loader.add_value("square_meters",square_meters )
        
        desc = "".join(response.xpath("//p[@class='comment']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        address_detail = response.xpath("//div[@class='path']/p/span/text()").extract_first()
        if address_detail:
            address=""
            if "Appartement" in address_detail:
                address=address_detail.split("Appartement")[1].strip()
                item_loader.add_value("address", address)
            elif "Studio" in address_detail:
                address=address_detail.split("Studio")[1].strip()
                item_loader.add_value("address", address)
            else: 
                item_loader.add_value("address", address_detail)
          
        deposit = response.xpath("//ul/li[contains(.,'Dépôt de garantie')]/span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ","").strip())

        utilities = response.xpath("//ul/li[contains(.,'Charges')]/span/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
         
        furnished = response.xpath("//ul/li[contains(.,'Meublé')]//text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        parking = response.xpath("//ul/li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        washing_machine = response.xpath("//ul/li[contains(.,'Lave-linge')]//text()").extract_first()
        if washing_machine:
            item_loader.add_value("washing_machine", True) 

        dishwasher = response.xpath("//ul/li[contains(.,'Lave-vaisselle')]//text()").extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True) 
         
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'show-carousel-thumbs')]//div/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "0158088080")
        item_loader.add_value("landlord_name", "SPEEDIMMO")

        yield item_loader.load_item()

