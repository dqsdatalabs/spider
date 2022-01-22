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
    name = 'habitat70_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.bienveo.fr/organisme-habitat-70/entite-habitat-70"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//div[@class='title']/a"):
            f_url = response.urljoin(item.xpath("./@href").get())
            yield Request(f_url, self.populate_item)
            seen = True
        
        if page == 1 or seen:
            p_url = f"https://www.bienveo.fr/organisme-habitat-70/entite-habitat-70?page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"property_type": response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = "".join(response.xpath("//div[@class='block-text']/span[@class='label']//text()").getall())
        if status and "achat" in status.lower():
            return

        item_loader.add_value("external_link", response.url)
        full_text = "".join(response.xpath("//div[@class='block-text']//span[@class='title']//text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return

        item_loader.add_value("external_source", "Habitat70_PySpider_france")

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//span[contains(@class,'label')]//following-sibling::span[contains(@class,'address')]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address.strip())

        city_zipcode = response.xpath("//span[contains(@class,'label')]//following-sibling::span[contains(@class,'address')]//text()[2]").get()
        if city_zipcode:
            try:
                city = city_zipcode.split(",")[0]
                zipcode = city_zipcode.split(",")[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            except :  
                city_zipcode = response.xpath("//span[contains(@class,'label')]//following-sibling::span[contains(@class,'address')]//text()[1]").get()
                if city_zipcode: 
                    city = city_zipcode.split(",")[0]
                    zipcode = city_zipcode.split(",")[1].strip()
                    item_loader.add_value("city", city)
                    item_loader.add_value("zipcode", zipcode)            
        else:
            city_zipcode = response.xpath("//span[contains(@class,'label')]//following-sibling::span[contains(@class,'address')]//text()").get()
            if city_zipcode:
                city = city_zipcode.split(",")[0]
                zipcode = city_zipcode.split(",")[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)  
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].split(",")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//div[contains(@class,'header')]//span[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//text()[contains(.,'Montant des charges')]").get()
        if utilities:
            utilities = "".join(filter(str.isnumeric, utilities.split("Montant des charges")[1].split("€")[0].split(".")[0].strip()))
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'desc')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'list-simple')]//li[contains(.,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[contains(@class,'list-simple')]//li[contains(.,'pièce')]//text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count.strip())
        
        images = [x for x in response.xpath("//div[contains(@class,'offer-gallery-images')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath("//div[contains(@class,'list-simple')]//li[contains(.,'ascenseur') or contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = "".join(response.xpath("//div[contains(@class,'content-left')]//img[contains(@alt,'Étage')]//parent::figure//following-sibling::div/text()").getall())
        if floor:
            if "rdc" in floor.lower():
                item_loader.add_value("floor", floor.strip())
            else:
                floor = floor.strip().split(" ")[0]
                item_loader.add_value("floor", floor)

        energy_label = response.xpath("//span[contains(.,'Consommation d’énergie')]//following-sibling::ul//li[contains(@class,'current')]//span[2]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'center')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center":[')[1].split(',')[0]
            longitude = latitude_longitude.split('center":[')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        landlord_name = ""
        landlord = response.xpath("//div[contains(@class,'desc')]//p//text()[contains(.,' Habitat') or contains(.,'HABITAT')]").get()
        if landlord:
            if "contact" in landlord.lower():
                if " au "in landlord:
                    landlord_name = landlord.lower().split("contact")[1].split("au")[0].replace("rapidement","").strip()
                    landlord_phone = landlord.split("au")[1].split("ou")[0].strip().replace("."," ")
                    item_loader.add_value("landlord_phone", landlord_phone)
                else:
                    if "ou" in landlord:
                        landlord_name = landlord.split("ou")[0].strip().split(" ")[-1]
            else: 
                if " au "in landlord:
                    landlord_phone = landlord.split("au")[1].split("ou")[0].strip().replace("."," ")
                    item_loader.add_value("landlord_phone", landlord_phone)
                else:
                    if "ou" in landlord:
                        landlord_phone = landlord.split("ou")[0].strip().split(" ")[-1]              
                        item_loader.add_value("landlord_phone", landlord_phone)
            
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name.capitalize())
            else:                
                item_loader.add_value("landlord_name", "HABITAT 70")
        else:                
            item_loader.add_value("landlord_name", "HABITAT 70")
            landlord = response.xpath("//div[contains(@class,'desc')]//p//text()[contains(.,' contactez') or contains(.,'CONTACTEZ')]").get()
            if landlord:
                if "contact" in landlord.lower():
                    if " au "in landlord:
                        landlord_name = landlord.lower().split("contact")[1].split("au")[0].replace("rapidement","").strip()
                        landlord_phone = landlord.split("au")[1].split("ou")[0].strip().replace("."," ")
                        item_loader.add_value("landlord_phone", landlord_phone)
                    else:
                        if "ou" in landlord:
                            landlord_name = landlord.split("ou")[0].strip().split(" ")[-1]
                else: 
                    if " au "in landlord:
                        landlord_phone = landlord.split("au")[1].split("ou")[0].strip().replace("."," ")
                        item_loader.add_value("landlord_phone", landlord_phone)
                    else:
                        if "ou" in landlord:
                            landlord_phone = landlord.split("ou")[0].strip().split(" ")[-1]              
                            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href,'mailto')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
