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
    name = 'admimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.admimmo.com/biens-a-louer/biens-a-louer.html"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='row']/div[@class='col-xs-12 col-sm-6 col-md-4 col-lg-3']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            prop_type = item.xpath(".//a/@title").get()
            if "APPARTEMENT" in prop_type :
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title.strip())
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Admimmo_PySpider_"+ self.country + "_" + self.locale)

        external_id="".join(response.url)
        if external_id:
            external_id = external_id.split("-")[2:3]
            item_loader.add_value("external_id", external_id)

        rent=response.xpath("//div[@class='block-price']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace("*",""))

        city=response.xpath("//h3[contains(@class,'block-sector')]//text()").extract_first()
        if city:
            city="".join(city.split(" ")[-1:])
            item_loader.add_value("city", city)

        address=response.xpath("//h3[@class='block-sector']//text()").extract_first()
        if address:
            item_loader.add_value("address", address)
        else:
            address = "".join(response.xpath("//div[@class='block-page property']//h1/text()").extract())
            if address:
                if " à " in address:
                    item_loader.add_value("address", address.split("à")[1].strip())
        
        # external_id = re.sub('\s{2,}', ' ', ("".join(response.xpath("//li[contains(.,'Mandat')]/b/text()").getall()).replace("\n",""))).strip()
        # if external_id:
        #     item_loader.add_value("external_id", rent.replace("n°",""))

        desc = "".join(response.xpath("//div[@class='block-html']//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip()) 
            if "parking" in desc :
                item_loader.add_value("parking", True)
            if "balcon" in desc :
                item_loader.add_value("balcony", True)
            if "terrasse" in desc :
                item_loader.add_value("terrace", True)
        
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/b/text()").extract_first()
        if square_meters :           
            item_loader.add_value("square_meters", square_meters.split("m")[0])
    
        utilities = response.xpath("(//div[contains(.,'Charges ')]/text())[11]").extract_first()
        if utilities :  
            item_loader.add_value("utilities", utilities.split("Charges")[1].split("€")[0].strip())
        else:
            utilities = response.xpath("//li[contains(.,'Montant des charges')]/b/text()").extract_first()
            if utilities :  
                item_loader.add_value("utilities", utilities.split("€")[0].strip())
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/b/text()").extract_first()
        if deposit :  
            dp=deposit.split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", str(dp))
        else:
            deposit = response.xpath("//li[contains(.,'DÉPÔT DE GARANTIE')]/b/text()").extract_first()
            if deposit :  
                dp=deposit.split("€")[0].strip().replace(".",",")
                item_loader.add_value("deposit", str(dp))

        
        room_count = response.xpath("//li[contains(.,'pièce')]/b/text()").extract_first()
        if room_count :
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'toilettes')]/b/text()").extract_first()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count)

        energy_label = response.xpath("//li[contains(.,'DPE')]/span/text()").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label.split("(")[0].strip())
     

        floor = response.xpath("//li[contains(.,'Etage')]/b/text()").extract_first()
        if floor :
            item_loader.add_value("floor", floor)

     
        item_loader.add_value("landlord_name", "Agence,Du Marche")
        item_loader.add_value("landlord_phone", "0160220572")
        item_loader.add_value("landlord_email", "contact@admimmo.com")
        
        images = [x for x in response.xpath("//div[contains(@class,'carousel-item')]//img[@class='d-block w-100']//@src").extract()]
        if images:
            item_loader.add_value("images", images)

        if item_loader.get_collected_values("room_count") and item_loader.get_collected_values("address") and square_meters:
            yield item_loader.load_item()