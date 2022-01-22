# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'ahrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Ahrealty_Com_PySpider_australia'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Block+Of+Units&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Duplex&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Flat&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",

                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Semi-detached&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",

                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.ahrealty.com.au/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
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
        for item in response.xpath("//li/div/a"):
            status = item.xpath(".//h3/text()").get()
            if status and "commercial" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("id=")[1].split("/")[0])

        title = response.xpath("//h1[contains(@class,'title')]//span//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        title_info = "".join(response.xpath("//div[@class='meta']//text()").getall())
        if title_info and "furnished" in title_info.lower():
            item_loader.add_value("furnished", True)
        else:
            furnished= response.xpath("//div[contains(@id,'main-content')]//p//text()[contains(.,'furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
        
        address = response.xpath("//div[contains(@class,'address')]//text()").get()
        if address:
            item_loader.add_value("address",address)
            city = address.split(",")[-1]
            item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(.,'$')]//text()").get()
        if rent:
            if "pw" in rent:
                rent = rent.split("pw")[0]
            elif "week" in rent:
                rent = rent.split("week")[0]
            else:
                rent = rent.replace("/"," ").strip().split(" ")[0]
            rent = rent.replace(",","").split("$")[-1].split('week')[0].split('per')[0].split('/')[0].strip()
            item_loader.add_value("rent", int(float(rent))*4)
        else:
            rent = response.xpath("//div[contains(@class,'property-header')]//span//text()[contains(.,'WEEK')]").get()
            if rent:
                item_loader.add_value("rent", rent.split("/")[0])
            
        item_loader.add_value("currency", "AUD" )
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[contains(@class,'meta')]//text()[contains(.,'Available')]").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split(" on")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        desc = " ".join(response.xpath("//div[contains(@id,'main-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        room_count = response.xpath("//i[contains(@class,'bed')]/parent::div[contains(@class,'title')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])

        bathroom_count = response.xpath("//i[contains(@class,'tint')]/parent::div[contains(@class,'title')]//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count )
        
        parking = response.xpath("//i[contains(@class,'car')]/parent::div[contains(@class,'title')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slide')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@id,'agent')]//h2//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        land_email = response.xpath("//div[@class='contact']/a/text()[contains(.,'@')]").get()
        if land_email:
            item_loader.add_value("landlord_email", land_email.strip())
        else:
            item_loader.add_value("landlord_email", "principal@ahrealty.com.au")
        
        landlord_phone = response.xpath("//div[contains(@id,'agent')]//a[contains(@href,'tel')]//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()