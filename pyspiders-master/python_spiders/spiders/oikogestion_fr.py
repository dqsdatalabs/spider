# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import re

class MySpider(Spider):
    name = 'oikogestion_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Oikogestion_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.oikogestion.fr/fr/toutes-les-annonces-immobilieres/?localisation=&types%5B%5D=Appartement&min=&max=",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'c-rental__link')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
    
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='l-container l-container--small'][1]//p//text()[contains(.,'sécurisé')]").get()
        if status:
            return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        if "parking-en-sous-sol" in response.url:
            return        
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title/text()").get()
        if title:
            if "Place de parking" in title or "Place de stationnement" in title:
                return
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//div[@class='p-rental__location']//text()").getall())
        if address:
            item_loader.add_value("address", address)
        
        city = response.xpath("//span[@class='p-rental__city']//text()").get()
        if city:
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//span[@class='p-rental__zipcode']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[@class='p-rental__amount']//text()").get()
        if rent:
            item_loader.add_value("rent", rent)
        elif response.xpath("//text()[contains(.,'Loyer :')]").get():
            item_loader.add_value("rent", response.xpath("//text()[contains(.,'Loyer :')]").get().split('Loyer :')[1].split('€')[0].strip().replace(' ', ''))
        item_loader.add_value("currency", "EUR")

        if response.xpath("//text()[contains(.,'LOCATION MEUBLE')]").get(): item_loader.add_value("furnished", True)
        
        desc = "".join(response.xpath("//p[@class='p-rental__description']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        room_count = response.xpath("//div[@class='p-rental__spec']//text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        if room_count.split(" ")[0] == "" or room_count.split(" ")[0] == None:
            return
        
        square_meters = "".join(response.xpath("//div[@class='p-rental__spec'][contains(.,'m²')]//text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        elif "m\u00b2" in desc:
            square_meters = desc.split("m\u00b2")[0].strip().split(" ")[-1].replace("(","")
            item_loader.add_value("square_meters", square_meters)
            
        available_date = response.xpath("//div[@class='p-rental__availability']/strong/text()").get()
        if available_date and "immédiate" in available_date.lower():
            available_date = datetime.now().strftime("%Y-%m-%d")
            item_loader.add_value("available_date", available_date)
        
        external_id = response.xpath("//div[@class='p-rental__ref']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())



        if "salle de bain" in desc:
            bathroom_count = desc.split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","").replace("au","").replace("r","")
            if (floor.replace("eme","")).isdigit():
                item_loader.add_value("floor", floor)
                    
        images = [ x for x in response.xpath("//div[contains(@class,'slider__items')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        utilities = response.xpath("//ul/li[contains(.,'charges ')]/text()").get()
        if utilities:
            uti = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", int(float(uti)))
        
        deposit = response.xpath("//ul/li[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].strip())
        
        item_loader.add_value("landlord_name", "Oiko Gestion")
        item_loader.add_value("landlord_phone", "01 40 57 69 80")
        
        yield item_loader.load_item()