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
    name = 'versalia_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.versalia-immobilier.com/locations/"]
    
    def parse(self, response):
        length = 0
        for item in response.xpath("//div[contains(@class,'textWithImage')]"):
            length = length + 1
            property_type = "".join(item.xpath(".//text()").getall())
            if property_type:
                if 'appartement' in property_type.lower() or 'duplex' in property_type.lower(): property_type = "apartment"
                elif 'maison' in property_type.lower() or 'résidence ' in property_type.lower(): property_type = "house"
                else: property_type = None
            
            if property_type: 
                item_loader = ListingLoader(response=response)
                item_loader.add_value("external_source", "Versalia_Immobilier_PySpider_france")
                item_loader.add_value("property_type", property_type)                
                item_loader.add_value("external_link", response.url)

                title = "+".join(response.xpath("//div[@class='hr']/parent::div/parent::div/following-sibling::div[1]/p[1]/span/text()[not(contains(.,'Merci')) and not(contains(.,'immobi'))]").getall())
                if title:
                    title = title.split("+")[length-1].strip()
                    item_loader.add_value("title", title)
                    if "m\u00b2" in title:
                        address = title.split("m\u00b2")[1].split("\u00a0")[0].split("1 ")[0].strip()
                        item_loader.add_value("address", address)
                        item_loader.add_value("city", address)
                    elif "Maison" in title:
                        address = title.split("Maison")[1].split("\u00a0")[0].split("1 ")[0].strip()
                        item_loader.add_value("address", address)
                        item_loader.add_value("city", address)
                    elif "duplex " in title:
                        address = title.split("duplex ")[1].split("\u00a0")[0].split("1 ")[0].strip()
                        item_loader.add_value("address", address)
                        item_loader.add_value("city", address)
                rent = item.xpath(".//div[@class='textwrapper']//text()[contains(.,'Loyer')]").get()
                if rent:
                    price = rent.split("=")[1].split("€")[0].replace(" ","")
                    item_loader.add_value("rent", price)
                    item_loader.add_value("currency", "EUR")
                utilities = item.xpath(".//div[@class='textwrapper']//text()[contains(.,'Loyer') and contains(.,'+')]").get()
                if utilities:
                    utilities = utilities.split("+")[-1].split("€")[0].replace(" ","")
                    item_loader.add_value("utilities", utilities)    
                deposit = item.xpath(".//div[@class='textwrapper']//text()[contains(.,'de garantie')]").get()
                if deposit:
                    deposit = deposit.split(":")[1].split("€")[0].replace(" ","")
                    item_loader.add_value("deposit", deposit)
                
                desc = " ".join(item.xpath(".//div[@class='textwrapper']//text()").getall())
                if desc:
                    desc = re.sub('\s{2,}', ' ', desc.strip())
                    item_loader.add_value("description", desc)
                
                if "chambre" in desc:
                    room_count = desc.split("chambre")[0].strip().split(" ")[-1].replace("dressing,\u00a0","")
                    if room_count.isdigit():    
                        item_loader.add_value("room_count", room_count)
                    elif "pièces "  in desc:
                        room_count = desc.split("pièces ")[0].strip().split(" ")[-1]
                        item_loader.add_value("room_count", room_count)
                if "m\u00b2" in desc:
                    square_meters = desc.split("m\u00b2")[0].strip().split(" ")[-1]
                    item_loader.add_value("square_meters", int(float(square_meters.replace(",","."))))
                                
                if "\u00e9tage" in desc:
                    floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1]
                    item_loader.add_value("floor", floor)
                
                images = [x for x in item.xpath(f"./..//div[contains(@class,'module-type-gallery')][{length}]//div[contains(@id,'lightbox-gallery')]//@src").getall()]
                item_loader.add_value("images", images)
                
                terrace = item.xpath("//ul/li[contains(.,'terrasse')]/text()").get()
                if terrace:
                    item_loader.add_value("terrace", True)
                    
                item_loader.add_value("landlord_name", "VERSALIA IMMOBILIER")
                item_loader.add_value("landlord_phone", "01 71 41 06 12")
                item_loader.add_value("landlord_email", "contact@versalia-immobilier.com")
                
                yield item_loader.load_item()