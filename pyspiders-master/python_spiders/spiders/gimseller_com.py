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
from datetime import datetime
import re

class MySpider(Spider):
    name = 'gimseller_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Gimseller_PySpider_france_fr'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.gimseller.com/fr/locations/"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='list']/div//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            if "maison" in follow_url or "duplex" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "house"})
            elif "appartement" in follow_url or "studio" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Gimseller_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = "".join(response.xpath("//p[@class='comment']/text()").extract())
        item_loader.add_value("description", desc.strip())

        if "garantie" in desc.lower():
            deposit=desc.lower().split("garantie")[1].replace(":","").strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)
        
        if "charges" in desc.lower():
            charges=desc.lower().split("charges")[1].replace(":","").strip().split(" ")[0]
            if charges.isdigit():
                item_loader.add_value("utilities", charges)
            else:
                if charges and ":" in charges:
                    charges=desc.lower().split("charges")[2].replace(":","").strip().split(" ")[0].strip()
                else:
                    charges=desc.lower().split("harges")[-1].replace(":","").strip().split("€")[0].split(',')[0].strip().split(' ')[-1].strip()
                if charges.isdigit():
                    item_loader.add_value("utilities", charges)
        
        if "piscine" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        
        if "balcon" in desc.lower():
            item_loader.add_value("balcony", True)
        
        price = response.xpath("//div[@class='price']/text()").get()
        if price:
            item_loader.add_value(
                "rent_string", price.replace(" ",""))
        
        if "Ref." in desc:
            external_id=desc.split("Ref.")[1].strip().split(" ")[0]
            item_loader.add_value("external_id", external_id)

        square = response.xpath(
            "//div[@class='content']/h2/text()"
        ).get()
        if square:
            try:
                item_loader.add_value(
                    "square_meters", square.split("-")[1].strip().split("m²")[0]
                )
            except:pass
        room_count = response.xpath(
            "//div[@class='content']/h2/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("-")[0].strip().split("pièce")[0])

        address = response.xpath("//div[@class='title']/h1/text()").get()
        if address:
            item_loader.add_value("address", address.split("- ")[1])
            item_loader.add_value("city", address.split("- ")[1].split(" ")[0])
            
        available_date = response.xpath(
            "//p[@class='comment']/text()[contains(.,'Disponible') or contains(.,'disponible')]").get()
        if available_date:     
            available_date = available_date.lower().split("disponible")[1].split(".")[0]      
            if "immédiatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
               
       
        
        floor = response.xpath("//p[@class='info']/text()[contains(.,'Étage')]").get()
        if floor:
            if "Dernier" in floor:
                item_loader.add_value("floor", floor.split("/")[1].strip().split(" ")[0].strip())
            else:
                item_loader.add_value("floor", floor.split(":")[1].strip().split("ème")[0].strip())
        
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='slide']/a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        energy=response.xpath("//div[@id='diagnostic']/img/@src").get()
        if energy:
            energy=energy.split("/")[-1]
            energy_label=energy_label_calculate(energy)
            if energy:
                item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_phone", "+33 4 93 35 76 53")
        item_loader.add_value("landlord_name", "GIM'SELLER Immobilier")

        yield item_loader.load_item()
    
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label