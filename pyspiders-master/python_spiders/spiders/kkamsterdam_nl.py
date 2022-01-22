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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'kkamsterdam_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Kkamsterdam_PySpider_netherlands"
    def start_requests(self):
        yield Request(
            "https://www.kkamsterdam.nl/nl-nl/woningaanbod/huur?mustbefurnished=true&orderby=3",
            callback=self.parse,
            meta={"furnished":True}
        )
        yield Request(
            "https://www.kkamsterdam.nl/nl-nl/woningaanbod/huur?mustbeupholstered=true&orderby=3",
            callback=self.parse,
            meta={"furnished":False}
        )
        

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'sys-property-link object_data')]"):
            p_type = item.xpath(".//div[@class='obj_type']/text()").get()
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
            else:
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"furnished":response.meta["furnished"], "p_type":p_type})
        
        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"furnished":response.meta["furnished"]}
            )   
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["p_type"])
        item_loader.add_value("furnished", response.meta["furnished"])

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h1/text()").get()
        if address:
            address = address.split(":")[1].strip()
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip().split(" ")
            zipcode = zipcode[0]+" "+zipcode[1]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)
            
        rent = response.xpath("//td[contains(.,'prij')]/following-sibling::td/text()").get()
        if rent:
            price = rent.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//td[contains(.,'Gebruiksoppervlakte')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//td[contains(.,' kamers')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0].strip())
        
        bathroom_count = response.xpath("//td[contains(.,'badkamer')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].strip())
        
        external_id = response.xpath("//td[contains(.,'Referentienummer')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='object-photos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//div[@id='object-floorplans']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        available_date = response.xpath("//td[contains(.,'Aanvaarding')]/following-sibling::td/text()").get()
        if available_date:
            if "Direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            available_date = available_date.replace("maandag", "").replace("woensdag","").replace("dinsdag","")
            if "Per" in available_date:
                available_date = available_date.split("Per")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//td[contains(.,'Borg')]/following-sibling::td/text()").get()
        if deposit:
            deposit = deposit.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//td[contains(.,'Servicekosten')]/following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.split(",-")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("utilities", utilities)
        
        furnished = response.xpath("//td[contains(.,'Inrichting')]/following-sibling::td/text()[contains(.,'Gemeubileerd') or contains(.,'Gestoffeerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        floor = response.xpath("//td[contains(.,'Woonlaag')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0].strip().replace("e",""))
        
        latitude_longitude = response.xpath("//script[contains(.,'center')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center: [')[1].split(',')[0]
            longitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        balcony = response.xpath("//td[contains(.,'balkon')]/following-sibling::td/text()[contains(.,'Ja')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//td[contains(.,'lift')]/following-sibling::td/text()[contains(.,'Ja')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        parking = response.xpath("//td[contains(.,'garage')]/following-sibling::td/text()[contains(.,'Ja')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//td[contains(.,'Energielabel')]/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "KK Amsterdam")
        item_loader.add_value("landlord_phone", "06-11149099")
        item_loader.add_value("landlord_email", "info@kkamsterdam.nl")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None