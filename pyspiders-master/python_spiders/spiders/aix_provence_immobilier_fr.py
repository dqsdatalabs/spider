# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'aix_provence_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Aix_Provence_Immobilier_PySpider_france'
    
    url = "https://aix-provence-immobilier.fr/fr/locations"
    
    def start_requests(self):
            yield Request(self.url,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//ul[contains(@class,'_list listing')]//figure/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            p_url = self.url+f"?page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Aix_Provence_Immobilier_PySpider_france", input_type="VALUE")
        item_loader.add_value("external_link", response.url.split("?")[0])
        property_type = response.xpath("//a[contains(@href,'property_type')]//text()").get()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        external_id = response.xpath("//li[contains(.,'Référence')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        title = "".join(response.xpath("//h1//text()").getall()) 
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h2//a//text()").get()
        if address: 
            address = address.split(",")[0].split(" ")[-1] 
            item_loader.add_value("address", address)
            item_loader.add_value("city", address) 

        square_meters = response.xpath("//li[contains(.,'Surface')]//span//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//p[contains(.,'Mois')]/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]//span//text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip().replace("\u202f","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Provision sur charges')]//span//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@id,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        dontalllow=" ".join(response.xpath("//p[contains(@id,'description')]//text()").getall())
        if dontalllow and "box standard en sous sol" in desc.lower():
            return 

        room_count = response.xpath("//li[contains(.,'Pièce')]//span//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Salle')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slider')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//p[contains(@id,'description')]//text()[contains(.,'Disponible')]").getall())
        if available_date:
            available_date = available_date.replace(".","").strip().split(" ")[-1]
            if not "immédiatement" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        furnished = response.xpath("//li[contains(.,'Meublé')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//li[contains(.,'Étage')]//span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        dishwasher = response.xpath("//li[contains(.,'Vaisselle')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[contains(.,'Lave-linge')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Agence Accord")
        item_loader.add_value("landlord_phone", "+33 4 42 26 99 89")
        item_loader.add_value("landlord_email", "transaction@agenceaccord.fr")
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower() or "house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None