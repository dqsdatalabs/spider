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
    name = 'claudiomarwan_eview_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    custom_settings = {
        "PROXY_ON" : False
    }
    external_source = "Claudiomarwan_Eview_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://claudiomarwan.eview.com.au/wp-json/api/listings/all?priceRange=&category=Unit%2CApartment&limit=18&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://claudiomarwan.eview.com.au/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&limit=18&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://claudiomarwan.eview.com.au/wp-json/api/listings/all?priceRange=&category=Studio&limit=18&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
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

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        if data["status"].upper() == 'SUCCESS':
            seen = True
            for item in data["results"]:           
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})

        if page == 2 or seen: 
            yield Request(response.url.split('&paged=')[0] + f"&paged={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        leased = response.xpath("//div[contains(@class,'price')]//text()[contains(.,'LEASED') or contains(.,'Application Approved')]").get()
        if leased:
            return
                
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("-")[-1])

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.split(" - ")[0].strip())
            item_loader.add_value("title", title)

        address = response.xpath("//title//text()").get()
        if address:
            address = address.split(" - ")[0].strip()
            item_loader.add_value("address", address.strip())

        city = response.xpath("//script[contains(.,'address')]//text()").get()
        if city:
            city = city.split('Locality": "')[1].split('"')[0]
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//script[contains(.,'address')]//text()").get()
        if zipcode:
            zipcode_region = zipcode.split('addressRegion": "')[1].split('"')[0]
            zipcode_code = zipcode.split('postalCode": "')[1].split('"')[0]
            item_loader.add_value("zipcode", zipcode_region + " " + zipcode_code)

        square_meters = response.xpath("//h3[contains(.,'Land Size')]//following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        deposit = "".join(response.xpath("//div[contains(@class,'price')]//text()[contains(.,'Bond')]").getall())
        if deposit:
            deposit = deposit.split("Bond")[1].split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)

        rent = "".join(response.xpath("//div[contains(@class,'price')]//text()[not(contains(.,'Bond')) and contains(.,'$')][normalize-space()]").getall())
        if rent:
            rent = rent.split("$")[1].split("-")[0].strip().replace(",","").strip()
            if "/" in rent:
                rent = rent.lower().split("/")[0].split("pw")[0].split(" ")[0]
            else:
                rent = rent.split(" ")[0]
            item_loader.add_value("rent", int(float(rent.replace("pw","")))*4)
        else:
            rent = response.xpath("//div[contains(@class,'price')]//text()[not(contains(.,'Bond')) and contains(.,'week')][normalize-space()]").get()
            if rent:
                rent = rent.split("-")[0].split("/")[0].strip().replace(",","").strip()
            item_loader.add_value("rent", int(float(re.sub(r'\D','',rent)))*4)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//div[contains(@class,'wpb')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = "".join(response.xpath("//span[contains(.,'Bed')]//parent::div/text()").getall())
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = "".join(response.xpath("//span[contains(.,'Bath')]//parent::div/text()").getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'listing-single-slider')]//@data-lazy").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = "".join(response.xpath("//span[contains(.,'Car')]//parent::div/text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//p//text()[contains(.,'Furnished')][contains(.,'Yes')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//script[contains(.,'lat')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('long":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'listing-agents')]//h2//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Claudio+Marwan Residential")
        
        landlord_phone = response.xpath("//div[contains(@class,'author-tel')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "(03) 8781 3823")

        landlord_email = response.xpath("//div[contains(@class,'author-email')]//@href").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[1]
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "claudiomarwan@eview.com.au")


        yield item_loader.load_item()