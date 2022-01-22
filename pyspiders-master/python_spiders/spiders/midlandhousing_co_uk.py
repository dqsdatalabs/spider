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
    name = 'midlandhousing_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'     
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://midlandhousing.co.uk/lettings?property_type=1&no_of_beds=0&price=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://midlandhousing.co.uk/lettings?property_type=2&no_of_beds=0&price=0",
                    
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://midlandhousing.co.uk/lettings?property_type=11&no_of_beds=0&price=0",
                    
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='actions']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Midlandhousing_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1])
        item_loader.add_value("property_type", response.meta.get('property_type'))

        address = response.xpath("//div[@class='container2']/h3/text()").get()
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1]
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            if city.strip():
                item_loader.add_value("city", city.strip())
            else:
                city = address.split(zipcode)[0].strip().strip(",").split(",")[-2]
                item_loader.add_value("city", city.strip())
            
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = response.xpath("//td[contains(.,'Rent PCM')]/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        room_count = response.xpath("//td[contains(.,'Bedroom')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//td[contains(.,'Bathroom')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//h2[contains(.,'Description')]/parent::div//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//td[contains(.,'Deposit')]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("£")[1])
        
        available_date = response.xpath("//td[contains(.,'Available')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//td[contains(.,'Furnished')]/following-sibling::td/text()").get()
        if furnished:
            if "Unfurnished" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        energy_label = response.xpath("//td[contains(.,'EPC')]/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.capitalize())
        
        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[2].split(',')[0]
            longitude = latitude_longitude.split('lng:')[2].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "MIDLAND HOUSING")
        item_loader.add_value("landlord_phone", "0121 773 5500")
        item_loader.add_value("landlord_email", "info@midlandhousing.com")
        
        yield item_loader.load_item()