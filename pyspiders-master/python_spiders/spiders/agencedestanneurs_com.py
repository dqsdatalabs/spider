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
    name = 'agencedestanneurs_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        formdata = {
            "transaction": "location",
            "cashmin": "",
            "cashmax": "",
            "surfmin": "",
            "surfmax": "",
            "send": "RECHERCHER  ►",
            "url": "/location-immobilier-marseille.html",
        }
        yield FormRequest("http://agencedestanneurs.com/index.php", 
                        formdata=formdata,
                        dont_filter=True, 
                        callback=self.parse)

    def parse(self, response):

        for item in response.xpath("//div[@id='listannonces']/div[not(contains(@class,'clear'))]"):
            property_type = item.xpath(".//span[@class='rougetann']/text()").get()
            follow_url = response.urljoin(item.xpath(".//form[@id='toolslist']/@action").get())
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agencedestanneurs_PySpider_france")

        external_id = response.xpath("//p[contains(.,'Référence')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        title = response.xpath("//h1//text()").get()
        if title:
            if "(" in title:                
                zipcode = title.split("(")[1].split(")")[0].strip()
                item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("title", title)
            
        
        address = response.xpath("//h3//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        else:
            address = response.xpath("//h1//text()").get()
            if address:
                address = address.split("appartement")[1].strip()
                city = address.split("(")[0].strip()
                zipcode = address.split("(")[1].split(")")[0]
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)

        rent = response.xpath("//tr[contains(.,'Loyer charges comprises')]//span//text()").get()
        if rent:
            rent = rent.replace(" ","").split("€")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//tr[contains(.,'Dépôt de garantie')]//td[contains(@class,'charges')]//text()").get()
        if deposit:
            deposit = deposit.strip().split("€")[0]
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//tr[contains(.,'charges')]//td[contains(@class,'charges')]//text()").get()
        if utilities:
            utilities = utilities.strip().split("€")[0]
            item_loader.add_value("utilities", utilities)
        
        square_meters = response.xpath("//tr[contains(.,'Surface')]//td[contains(@class,'tdrecap2')]//text()").get()
        if square_meters:
            square_meters = square_meters.replace(",",".").strip().split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//tr[contains(.,'Chambres')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'aucune'))]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//tr[contains(.,'Pièces')]//td[contains(@class,'tdrecap2')]//text()").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//tr[contains(.,'Salle')]//td[contains(@class,'tdrecap2')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        desc = " ".join(response.xpath("//div[contains(@id,'descfiche')]/div/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        elevator = response.xpath("//tr[contains(.,'Ascenseur')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'Non'))]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//tr[contains(.,'Balcon')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'Non'))]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//tr[contains(.,'Terrasse')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'Non'))]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//tr[contains(.,'Parking')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'Non'))]").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//tr[contains(.,'Meublé')]//td[contains(@class,'tdrecap2')]//text()[not(contains(.,'Non'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//tr[contains(.,'Étage')]//td[contains(@class,'tdrecap2')]//text()").get()
        if floor:
            floor = floor.replace("er","")
            if "(" in floor:
                floor = floor.split("(")[0].strip()
                item_loader.add_value("floor", floor)
            else:
                if floor.isdigit():
                    item_loader.add_value("floor", floor)
        
        energy_label = response.xpath("//span[contains(@class,'enerview dpeE')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//div[contains(@id,'slidefiche')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "AGENCE IMMOBILIÈRE DES TANNEURS")
        landlord_phone = response.xpath("//div[@id='spec']//div[@id='contactfiche']//img[contains(@src,'phone')]/../text()").get()
        if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.strip().replace(".", " "))
        item_loader.add_value("landlord_email", "achatvente@tanneurs.immo")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None