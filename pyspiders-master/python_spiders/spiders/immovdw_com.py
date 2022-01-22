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
import math

class MySpider(Spider):
    name = 'immovdw_com'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source = "Immovdw_PySpider_belgium"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.immovdw.com/nl/te-huur?type=Appartement%20Nieuwbouwproject",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immovdw.com/nl/te-huur?type=Woning",
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
        for item in response.xpath("//div[contains(@class,'pand-teaser')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[contains(@class,'item_left')][contains(.,'Adres')]/following-sibling::div[1]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split("-")[-1].strip().split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div[contains(@class,'item_left')][contains(.,'Prij')]/following-sibling::div[1]/text()[not(contains(.,'Prijs op aanvraag'))]").get()
        if rent:
            price = rent.split("€")[1].strip().replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[contains(@class,'subtitle')][contains(.,'slaapkamer')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("slaap")[0].strip())
        
        bathroom_count = response.xpath("//div[contains(@class,'item_left')][contains(.,'Badkamer')]/following-sibling::div[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall())
        if desc:
            # desc = re.sub('\s{2,}', ' ', desc)
            if "www.immovdw.com" in desc.lower():
                desc="".join(desc.split("www.immovdw.com")[0])
                item_loader.add_value("description", desc)
            else:
                item_loader.add_value("description", desc)
        
        square_meters = response.xpath("//div[contains(@class,'subtitle')][contains(.,'opp.')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
        elif "m\u00b2" in desc:
            square_meters = desc.split("m\u00b2")[1].strip().split(" ")[-1]
            if square_meters.isdigit():
                item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
            
        energy_label = response.xpath("//div[contains(@class,'item_left')][contains(.,'label')]/following-sibling::div[1]/img/@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("epc_")[1].split(".")[0].upper())
        
        elevator = response.xpath("//div[contains(@class,'item_left')][contains(.,'Lift')]/following-sibling::div[1]/text()").get()
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            elif "nee" in elevator.lower():
                item_loader.add_value("elevator", False)
        
        terrace = response.xpath("//div[contains(@class,'item_left')][contains(.,'Terras')]/following-sibling::div[1]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[contains(@class,'item_left')][contains(.,'Garage')]/following-sibling::div[1]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit = response.xpath("//div[contains(@class,'item_left')][contains(.,'Huurwaarborg')]/following-sibling::div[1]/text()").get()
        if deposit:
            deposit = deposit.split("€")[1].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//div[contains(@class,'item_left')][contains(.,'lasten')]/following-sibling::div[1]/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].strip()
            item_loader.add_value("utilities", utilities)
        
        available_date = "".join(response.xpath("//div[contains(@class,'item_left')][contains(.,'Beschikbaar')]/following-sibling::div[1]//text()").getall())
        if available_date:
            match = re.search(r'(\d+/\d+/\d+)', available_date)
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            elif available_date.strip().lower() == "onmiddellijk":
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.replace("onmiddellijk /","").strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        external_id = "".join(response.xpath("//div[contains(@class,'item_left')][contains(.,'Referentie')]/following-sibling::div[1]//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        images = [x for x in response.xpath("//div[@class='image-wrapper']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//div[@class='info-wrapper']/h3/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//div[@class='info-wrapper']/a[@class='mobile']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        email = response.xpath("//div[@class='info-wrapper']/a[contains(.,'@')]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        
        
        
        yield item_loader.load_item()