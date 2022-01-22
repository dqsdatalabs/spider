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
    name = 'sidimex_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sidimex.be/nl/te-huur?view=list&page=1&ptype=3&skipmarquees=3,4",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sidimex.be/nl/te-huur?view=list&page=1&ptype=1&skipmarquees=3,4",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item-inner']"):
            status = item.xpath("./div[contains(@class,'prop-marquee')]/text()").get()
            if status and "verhuurd" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'nav next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Sidimex_PySpider_belgium")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("&id=")[-1].split("&")[0])
        
        title = response.xpath("//div[@class='descr']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='field']/div[contains(.,'Adres')]/following-sibling::div/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip().split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
                    
        square_meters = response.xpath("//div[@class='field']/div[contains(.,'Bewoonbare opp.')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//div[@class='field']/div[contains(.,'SLPK')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='field']/div[contains(.,'badkamer')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//div[@class='field']/div[contains(.,'Prij')]/following-sibling::div/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].strip())
            item_loader.add_value("currency", "EUR")
        
        energy_label = response.xpath("//div[@class='field']/div[contains(.,'EPC')]/following-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[0])
        
        available_date = response.xpath("//div[@class='field']/div[contains(.,'Beschikbaarheid')]/following-sibling::div/text()").get()
        if available_date:
            if "Onmiddellijk" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//div[@class='field']/div[contains(.,'Verdieping')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("/")[0])
        
        terrace = response.xpath("//div[@class='field']/div[contains(.,'Terras')]/following-sibling::div/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//div[@class='field']/div[contains(.,'Lift')]/following-sibling::div/text()").get()
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator", True)
            elif "ne" in elevator.lower():
                item_loader.add_value("elevator", False)
        
        if title and "gemeubeld" in title.lower():
            item_loader.add_value("furnished", True)
        
        utilities = response.xpath("//div[@class='html-descr']//p//text()[contains(.,'kosten')]").get()
        if utilities:
            count = utilities.count("€")
            if "euro" in utilities:
                utilities = utilities.split("euro")[0].strip().split(" ")[-1]
            elif count ==1:
                utilities2 = utilities.split("€")[1].strip().split(" ")[0]
                if utilities2.isdigit():
                    item_loader.add_value("utilities", utilities2)
                else:
                    utilities = utilities.split("€")[0].strip().split(" ")[-1]
                    if utilities.isdigit():
                        item_loader.add_value("utilities", utilities)
            elif count >1:
                utilities2 = utilities.split("€")[1].strip().split(" ")[-1]
                if utilities2.isdigit():
                    item_loader.add_value("utilities", utilities2)
                else:
                    utilities = utilities.split("€")[2].strip().split(" ")[0].replace("/mnd","")
                    if utilities.isdigit():
                        item_loader.add_value("utilities",utilities)
        
        desc = " ".join(response.xpath("//div[@class='html-descr']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='camera-wrap']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'MyCustomMarker(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("MyCustomMarker([")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("MyCustomMarker([")[1].split(",")[1].split("]")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        phone = "".join(response.xpath("//p[@class='manager-info']/text()").getall())
        if phone:
            item_loader.add_value("landlord_name", phone.split("-")[0].strip())
            item_loader.add_value("landlord_phone", phone.split("-")[1].strip())
            
        email = "".join(response.xpath("//p[@class='manager-info']/a/text()").getall())
        if email:
            item_loader.add_value("landlord_email", email)
      
        yield item_loader.load_item()