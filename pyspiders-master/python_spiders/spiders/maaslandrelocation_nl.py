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
    name = 'maaslandrelocation_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Maaslandrelocation_PySpider_netherlands'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://maaslandrelocation.nl/nl/properties/search/5e94y92r/apartments-houses",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://maaslandrelocation.nl/nl/properties/search/pqgxjy9v/kamers",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='results']/div/a"):
            status = item.xpath(".//span[@class='let']/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.url
        if get_p_type_string(property_type):
            prop_type = get_p_type_string(property_type)
            item_loader.add_value("property_type", prop_type)
        else:
            return 
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Maaslandrelocation_PySpider_netherlands")
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//p[contains(@class,'location')]/text()").get()
        if address:
            address = address.strip().split(" ")[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
                
        rent = response.xpath("//dl/dt[.='huur']/following-sibling::dd/text()").get()
        if rent:
            price = rent.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//dl/dt[contains(.,'oppervlakte')]/following-sibling::dd/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        deposit = response.xpath("//dl/dt[.='borg']/following-sibling::dd/text()").get()
        if deposit:
            deposit = deposit.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//dl/dt[contains(.,'kosten')]/following-sibling::dd/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0].split("€")[1].strip())
            
        desc = " ".join(response.xpath("//p[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if response.meta.get("property_type") == "room":
            item_loader.add_value("room_count", "1")
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        elif "slaapkamer" in desc:
            room_count = desc.split("slaapkamer")[0].strip().split(" ")[-1]
            if "Twee" in room_count:
                item_loader.add_value("room_count", "2")
        
        images = [x for x in response.xpath("//div[@class='photos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'mapToken')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"l":[')[1].split(',')[0]
            longitude = latitude_longitude.split('"l":[')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        available_date = response.xpath("//dl/dt[contains(.,'beschikbaar')]/following-sibling::dd/text()").get()
        if available_date:
            if "direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date.split("per")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id = response.xpath("//dl/dt[contains(.,'referentie')]/following-sibling::dd/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        if "verdieping" in desc:
            floor = desc.split("verdieping")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        parking = response.xpath("//dl/dt[contains(.,'parkeer')]/following-sibling::dd/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        features = "".join(response.xpath("//dl/dt[contains(.,'bijzonderheden')]/following-sibling::dd//text()").getall())
        if features:
            if "Wasmachineaansluiting" in features:
                item_loader.add_value("washing_machine", True)
                
        item_loader.add_value("landlord_name", "Maasland Relocation")
        item_loader.add_value("landlord_phone", "31 43 321 59 13")
        item_loader.add_value("landlord_email", "info@maaslandrelocation.nl")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    else:
        return None