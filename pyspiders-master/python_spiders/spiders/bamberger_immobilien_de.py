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
import dateparser

class MySpider(Spider):
    name = 'bamberger_immobilien_de'
    execution_type='testing'
    country = 'germany'
    locale ='de'
    external_source = "Bamberger_Immobilien_PySpider_germany"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bamberger-immobilien.de/mietobjekte/wohnungen/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bamberger-immobilien.de/mietobjekte/haeuser/",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[@class='row immo-list']"):
            # no detail page

            item_loader = ListingLoader(response=response)
            title = item.xpath("./section/h2/text()").get()
            item_loader.add_value("title", title)    

            images = [x for x in item.xpath("./section/a/img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
            
            ext_id = item.xpath("./section/h2/following-sibling::text()[contains(.,'Objekt-Nr.:')]").get()
            if ext_id:
                ext_id =  ext_id.split("Objekt-Nr.:")[-1].split("Größe")[0].strip()
                item_loader.add_value("external_id",ext_id)
            ext_link = response.url+f"#{ext_id}"
            item_loader.add_value("external_link", ext_link)
            item_loader.add_value("property_type", response.meta["property_type"])
            item_loader.add_value("external_source", self.external_source)    

            sq = item.xpath("./section//text()[contains(.,'Größe')]").get() #Größe: 70,00 qm
            if sq:
                sqm = sq.split("Größe:")[-1].split(",")[0].strip()
                address = sq.split("qm")[-1].strip()
                zipcode = sq.split("qm")[-1].strip().split(" ")[0]
                city = sq.split("qm")[-1].strip().split(" ")[-1].split("-")[-1]
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("square_meters", sqm)

            price = item.xpath("./section//text()[contains(.,'Kaltmiete')]").get()
            if price:
                rent = price.split("Kaltmiete:")[-1].split(",")[0]
                item_loader.add_value("rent", rent)

            item_loader.add_value("currency", "EUR")

            deposit = item.xpath("./following-sibling::div[@class='row immo-list details'][1]//text()[contains(.,'Nebenkosten:')]").get()
            if deposit:
                deposit = deposit.split("Nebenkosten:")[-1].split(",")[0]
                item_loader.add_value("deposit",deposit.strip())

            room = item.xpath("./following-sibling::div[@class='row immo-list details'][1]//text()[contains(.,'Zimmer:')]").get()
            if room:
                room = room.split("Zimmer:")[-1].split(".")[0]
                item_loader.add_value("room_count",room.strip())

            energy = item.xpath("./following-sibling::div[@class='row immo-list details'][1]//text()[contains(.,'Effizienzklasse')]").get()
            if energy:
                energy = energy.split("Effizienzklasse:")[-1].strip()
                item_loader.add_value("energy_label",energy.strip())

            desc = " ".join(item.xpath("./following-sibling::div[@class='row immo-list details'][1]/div/div//text()").getall())
            if desc:
                item_loader.add_value("description",desc.strip())
            
            item_loader.add_value("landlord_name", "Bamberger Immobilien")
            item_loader.add_value("landlord_email", "info@bamberger-immobilien.de")
            item_loader.add_value("landlord_phone", "+49 (0) 6446 - 6688")
            yield item_loader.load_item()
 
  