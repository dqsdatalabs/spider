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
    name = 'housingsolutions_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.housingsolutions.nl/huurwoningen-2?house_type=Appartement&order=online_date&region=breda",

                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.housingsolutions.nl/huurwoningen-2?house_type=2-onder-1-kap%20woning&region=breda&order=online_date",
                    "https://www.housingsolutions.nl/huurwoningen-2?house_type=Hoekhuis&order=online_date&region=breda",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.housingsolutions.nl/huurwoningen-2?house_type=Studio&order=online_date&region=breda",
                ],
                "property_type" : "studio"
            },
            {
                "url" : [
                    "https://www.housingsolutions.nl/huurwoningen-2?house_type=Kamer&order=online_date&region=breda",
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
        page = response.meta.get("page", 2)
        try:
            total_page = int(response.xpath("//div[@class='pagination-block']//ul/li[last()]/a//text()").get().strip())
        except:
            total_page = 1
        for item in response.xpath("//div[@class='photo']"):
            status = item.xpath("./small//text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if page <= total_page:
            p_url = response.url.split("&page=")[0] + f"&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Housingsolutions_PySpider_netherlands")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id",response.url.split("id=")[1])
        
        title = " ".join(response.xpath("//div[contains(@class,'text')]/h1//text()").getall())
        if title:
            item_loader.add_value("title", title)
        rent = ""
        if "€" in title:
            rent = title.split("€")[1].split(",-")[0]
            item_loader.add_value("rent", rent.strip())
            item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//li/span[contains(.,'Plaat')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        square_meters = response.xpath("//li/span[contains(.,'Oppervlakte')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//li/span[contains(.,'Slaapkamer')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            prop_type = response.meta.get('property_type')
            if prop_type == "studio" or prop_type == "room":
                item_loader.add_value("room_count", "1")
        
        furnished = response.xpath("//li/span[contains(.,'Interieur')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        available_date = response.xpath("//li/span[contains(.,'Beschikbaar')]/text()").get()
        if available_date:
            item_loader.add_value("available_date", available_date)
        
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//a/@title/parent::a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        utilities = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'servicekosten') and contains(.,'€')]").get()
        if utilities:
            utilities = utilities.split("€")[1].split(",-")[0].strip()
            item_loader.add_value("utilities", utilities)
        elif "G/W/L" in desc:
            utilities = desc.split("G/W/L")[1].split(",-")[0].strip().split(" ")[-1]
            utilities = int(utilities)-int(rent.strip())
            item_loader.add_value("utilities", utilities)
        
        if "verdieping" in desc.lower():
            floor = desc.lower().split("verdieping")[0].strip().split(" ")[-1]
            if "eerste" in floor:
                item_loader.add_value("floor", floor)
        elif "etage" in desc:
            floor = desc.split("etage")[0].strip().split(" ")[-1]
            if "eerste" in floor:
                item_loader.add_value("floor", floor)
                
        if "energielabel" in desc:
            energy_label = desc.split("energielabel")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label.replace(".",""))
        
        item_loader.add_value("landlord_name", "HOUSING SOLUTIONS")
        item_loader.add_value("landlord_phone", "0181 846959")
        
        yield item_loader.load_item()