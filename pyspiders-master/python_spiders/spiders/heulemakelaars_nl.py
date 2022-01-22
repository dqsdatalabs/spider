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
    name = 'heulemakelaars_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.heulemakelaars.nl/nl/realtime-listings/consumer"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["isRentals"] and "verhuurd" not in item["status"].lower():
                follow_url = response.urljoin(item["url"])
                p_type = item["type"]
                if get_p_type_string(p_type):
                    prop_type = get_p_type_string(p_type)
                else:
                    p_type = item["mainType"]
                    if get_p_type_string(p_type):
                        prop_type = get_p_type_string(p_type)
                    else:
                        continue
                
                latitude = str(item['lat']) if 'lat' in item.keys() else None
                longitude = str(item['lng']) if 'lng' in item.keys() else None
                balcony = item['balcony'] if 'balcony' in item.keys() else None
                furnished = item['isFurnished'] if 'isFurnished' in item.keys() else None
                room_count = item['bedrooms'] if 'bedrooms' in item.keys() else None
                square_meters = item['livingSurface'] if 'livingSurface' in item.keys() else None
                rent = item['rentalsPrice'] if 'rentalsPrice' in item.keys() else None
                city = item['city'] if 'city' in item.keys() else None
                zipcode = item['zipcode'] if 'zipcode' in item.keys() else None

                street = item['address'] if 'address' in item.keys() else None
                district = item['district'] if 'district' in item.keys() else None
                country = item['country'] if 'country' in item.keys() else None
                address = ""
                if street:
                    address += street + " "
                if district:
                    address += district + " "
                if city:
                    address += city + " "
                if zipcode:
                    address += zipcode + " "
                if country:
                    address += country + " "
                address = address.strip()
                
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type, "address": address, "latitude": latitude, "longitude": longitude,
                                "balcony": balcony, "furnished": furnished, "room_count": room_count, "square_meters": square_meters, "rent": rent, "city": city, "zipcode": zipcode})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_source", "Heulemakelaars_PySpider_netherlands")

        if response.meta.get("address"): item_loader.add_value("address", response.meta.get("address"))
        if response.meta.get("latitude"): item_loader.add_value("latitude", response.meta.get("latitude"))
        if response.meta.get("longitude"): item_loader.add_value("longitude", response.meta.get("longitude"))
        if response.meta.get("balcony"): 
            if response.meta.get("balcony").strip().lower() == 'ja':
                item_loader.add_value("balcony", True)
            elif response.meta.get("balcony").strip().lower() == 'nee':
                item_loader.add_value("balcony", False)
        if response.meta.get("furnished"): item_loader.add_value("furnished", response.meta.get("furnished"))
        if response.meta.get("room_count"): item_loader.add_value("room_count", response.meta.get("room_count"))
        if response.meta.get("square_meters"): item_loader.add_value("square_meters", response.meta.get("square_meters"))
        if response.meta.get("rent"): 
            item_loader.add_value("rent", response.meta.get("rent"))
            item_loader.add_value("currency", 'EUR')
        if response.meta.get("city"): item_loader.add_value("city", response.meta.get("city"))
        if response.meta.get("zipcode"): item_loader.add_value("zipcode", response.meta.get("zipcode"))
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@id,'omschrijving')]//div[contains(@class,'content-content')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        else:
            description = " ".join(response.xpath("//h2[contains(.,'Omschrijving')]/following-sibling::div//text()").getall()).strip()
            if description:
                item_loader.add_value("description", description.replace('\xa0', ''))

        bathroom_count = response.xpath("//dt[contains(.,'Aantal badkamer')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//dt[contains(.,'Status')]/following-sibling::dd[1]/text()").get()
        if available_date:
            available_date = 'now' if available_date.strip().lower() == 'beschikbaar' else available_date.strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//dt[contains(.,'Energielabel')]/following-sibling::dd[1]/text()").get()
        if energy_label:
            if energy_label.strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.strip().upper())
        
        floor = response.xpath("//dt[contains(.,'Verdiepingen')]/following-sibling::dd[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        pets_allowed = response.xpath("//dt[contains(.,'Huisdieren toegestaan')]/following-sibling::dd[1]/text()").get()
        if pets_allowed:
            if pets_allowed.strip().lower() == 'ja':
                item_loader.add_value("pets_allowed", True)
            elif pets_allowed.strip().lower() == 'nee':
                item_loader.add_value("pets_allowed", False)

        elevator = response.xpath("//br/following-sibling::text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//br/following-sibling::text()[contains(.,'Dakterras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        washing_machine = response.xpath("//br/following-sibling::text()[contains(.,'Wasmachine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        utilities = response.xpath("//dt[contains(.,'Servicekosten')]/following-sibling::dd[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].strip())

        deposit = response.xpath("//text()[contains(.,'Waarborgsom:')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].strip().replace('.', ''))

        water_cost = response.xpath("//text()[contains(.,'Voorschot water')]").get()
        if water_cost:
            item_loader.add_value("water_cost", water_cost.split('€')[-1].split(',')[0].strip().replace('.', ''))

        heating_cost = response.xpath("//text()[contains(.,'Voorschot gas')]").get()
        if heating_cost:
            item_loader.add_value("heating_cost", heating_cost.split('€')[-1].split(',')[0].strip().replace('.', ''))

        item_loader.add_value("landlord_name", "MAR.J.HEULE Makelaardij")
        item_loader.add_value("landlord_phone", "020 - 676 ​​66 33")
        item_loader.add_value("landlord_email", "info@heulemakelaars.nl")
        
        status = response.xpath("//span[contains(@class,'status')]/text()[contains(.,'Verkocht')]").get()
        if not status:
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