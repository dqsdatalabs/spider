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
from datetime import datetime

class MySpider(Spider):
    name = 'maisonsetappartements_com'
    execution_type='testing'
    country='france'
    locale='fr' 
    external_source='Maisonsetappartements_PySpider_france'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.maisonsetappartements.fr/views/Search.php?lang=fr&TypeAnnonce=LOC&TypeBien=APP&villes=2123&departement=&quartier=&bdgMin=&bdgMax=&surfMin=&surfMax=&nb_piece=&nb_km=&keywords=", "property_type": "apartment"},
            {"url": "https://www.maisonsetappartements.fr/views/Search.php?lang=fr&TypeAnnonce=LOC&TypeBien=TER&villes=2123&departement=&quartier=&bdgMin=&bdgMax=&surfMin=&surfMax=&nb_piece=&nb_km=&keywords=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@id,'photo_')]"):
            room_count = item.xpath(".//span[@itemprop='numberOfRooms']/text()").get()
            square_meters = item.xpath(".//span[@itemprop='floorSize']/text()").get()
            f_url = response.urljoin(item.xpath(".//a[@class='cookieRoom']/@href").get())
            yield Request(f_url, self.populate_item, meta={"property_type": response.meta.get('property_type'), "room_count": room_count, "square_meters": square_meters})
        
        next_page = response.xpath("//li[@class='nextP']/a/@href").get()
        if next_page:
            yield Request(
                next_page,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Maisonsetappartements_PySpider_"+ self.country)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent = response.xpath("//span[@class='newPrice']//text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
    
        room = response.xpath("//h2[@class='subtitle']//text()").get()
        if room:
            room="".join(room.split("-")[1:2])
            item_loader.add_value("room_count", room.split("Pièces")[0])
            if "meuble" in room.lower():
                item_loader.add_value("furnished", True)

                
        item_loader.add_value("square_meters", response.meta.get("square_meters"))
        
        address = response.xpath("//span[@class='infosSec']/img[contains(@src,'Localisa')]/parent::span/text()").get()
        if address:
            item_loader.add_value("address", address.replace("(","").replace(")","").replace("\u00a0"," "))
            item_loader.add_value("city", address.split("\u00a0")[0])
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
        
        external_id = response.xpath("//span[@class='infosSec'][contains(.,'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        desc = "".join(response.xpath("//p[@class='descr']//text()").getall())
        if desc:
            desc=desc.split("Tarifs de location")[0]
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
            
        match = re.search(r'(\d+/\d+/\d+)', desc.replace(".","/"))
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        elif "disponible imm\u00e9diatement" in desc.lower():
            available_date = datetime.now()
            item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d")) 
        elif "disponible" in desc.lower():
            available_date = desc.lower().split("disponible")[1].split("afficher")[0].replace("mi","").replace("le","").strip()
            available_date = available_date.split(".")[0].replace("début ","").replace("au ","")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)        
            
        deposit = "".join(response.xpath("//ul/li[contains(.,'garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split(":")[1].split(",")[0].replace(" ", "")
            item_loader.add_value("deposit", deposit)
        
        utilities = "".join(response.xpath("//ul/li[contains(.,'Charges ')]/text()[not(contains(.,'oui'))]").getall())
        if utilities:
            utilities = utilities.split(":")[1].split(",")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        else:
            utilities = "".join(response.xpath("//ul/li[contains(.,'rHonoraires')]/text()").getall())
            if utilities:
                utilities = utilities.split(":")[1].split(",")[0].strip()
                if utilities.isdigit():
                    item_loader.add_value("utilities", utilities)
                
        energy_label = response.xpath("//div[@id='dpe_value']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        furnished = response.xpath("//h2[contains(.,' meublé') or contains(.,'Meublé')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        images = [ x for x in response.xpath("//input[contains(@id,'mini')]/@value").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = response.xpath("//a[@class='link_agence']//text()").get()
        if landlord_name:  
            item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", "contact@maisonsetappartements.fr")
        item_loader.add_value("landlord_phone", "04 93 62 25 35")

        not_available = response.xpath("//div[@class='typePerime']").get()
        if not_available:
            return
        
        room_count = response.xpath("//span[@itemprop='numberOfRooms']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        else:
            item_loader.add_value("room_count",1)
        
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