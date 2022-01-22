# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'agence_couzinet_fr'
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
            
            yield FormRequest(url="http://www.agence-couzinet.fr/fr/recherche/",
                            callback=self.parse,
                            formdata=payload,
                            meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='ad']/a/@href").getall():
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
        item_loader.add_value("external_source", "Agence_Couzinet_PySpider_france")
    
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if " MEUBLE" in title.upper():
                item_loader.add_value("furnished", True) 

                        

        floor = response.xpath("//li[text()='Etage ']/span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0].replace("ème","").strip())
        external_id = response.xpath("//p[@class='comment']//text()[contains(.,'Ref.')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref.")[-1].strip())
        city = response.xpath("//div[@class='path']/p/span/text()").get()
        if city:
            address= " ".join(city.split(" ")[1:]).strip()
            item_loader.add_value("city", address.strip())
            item_loader.add_value("address", address)
        rent = response.xpath("//h2[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))   

        description = "".join(response.xpath("//p[@class='comment']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        bathroom_count = response.xpath("//li[contains(.,'Salle de bains')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        room_count = response.xpath("//li[contains(.,'Chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else:
            room_count = response.xpath("//li[text()='Pièces ']/span/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])

        square_meters = response.xpath("//li[text()='Surface ']/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        deposit = response.xpath("//li[text()='Dépôt de garantie']/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").strip())
        item_loader.add_xpath("utilities", "//li[text()='Charges']/span/text()")

        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        dishwasher = response.xpath("//li[contains(.,'Lave-vaisselle')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        washing_machine = response.xpath("//li[contains(.,'Lave-linge')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
      
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'show-carousel-thumbs')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "COUZINET IMMOBILIER")
        item_loader.add_value("landlord_phone", "+33 (0)5 34 31 09 74")
        item_loader.add_value("landlord_email", "contact@agence-couzinet.fr")
        yield item_loader.load_item()

