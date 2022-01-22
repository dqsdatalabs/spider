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
    name = 'jrlanding_com_au'
    execution_type='testing'
    country='australia'  
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Apartment&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Unit&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Terrace&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=House&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Townhouse&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Villa&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.jrlanding.com.au/rent?search=&listing_type=rent&property_type=Studio&bedrooms=&bathrooms=&min_rent=0&min_price=0&max_rent=&max_price=",
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
        for item in response.xpath("//div[@class='img-wrap']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Jrlanding_Com_PySpider_australia")   
        status=response.xpath("//div[@class='status']/ul[@class='meta-list']/li/a/text()[.='Let!']").get()
        if status:
            return 
            
        deposit_taken = response.xpath("//div[@class='price']/text()[contains(.,'Deposit') or contains(.,'Leased in ')]").get()
        if deposit_taken:
            return   
        item_loader.add_xpath("title","//h1[@class='page-title']/text()")
        item_loader.add_value("external_id", response.url.split("property_id=")[-1])
        item_loader.add_xpath("room_count", "//li[i[contains(@class,'flaticon-person1')]]/text()")
        item_loader.add_xpath("bathroom_count", "//li[span[contains(@class,'flaticon-shower')]]/text()")
        rent = response.xpath("//div[@class='price']/text()[.!='Contact for price' and contains(.,'$')]").get()
        if rent:
            if "week" in rent.lower() or "pw" in rent.lower():
                rent = rent.split("$")[-1].lower().split("p")[0].split("week")[0].split("-")[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)) * 4)
                item_loader.add_value("currency", 'AUD')
            else:
                rent = rent.split("$")[-1].strip()
                if "|" in rent:
                    rent = rent.split("|")[0].strip()
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'AUD')
        price1=response.xpath("//div[@class='price']/text()").get()
        if price1 and "tenant secured" in price1.lower():
            return
        if price1 and "tenants secured" in price1.lower():
            return
    

 
        address = response.xpath("//h1[@class='page-title']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city.strip()) 
    
        parking = response.xpath("//li[i[contains(@class,'flaticon-car')]]/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//div[@class='title']//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        swimming_pool = response.xpath("//li[contains(.,'Swimming Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        pets_allowed = response.xpath("//li[contains(.,'Pet Friendly')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        script_map = response.xpath("//script[contains(.,' L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split(" L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        available_date = response.xpath("//div[@id='availdate']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1].replace("from","").strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='section']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@class='carousel-wrap']//img/@src[not(contains(.,'-floorplan'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='carousel-wrap']//img/@src[contains(.,'-floorplan')]").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        landlord_name = response.xpath("//div[contains(@class,'agent-card')][1]/div/div[@class='title']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'agent-card')][1]//li/a[contains(@href,'tel')]/text()")
        item_loader.add_value("landlord_email", "mirandawu@jrlanding.com.au")

        yield item_loader.load_item()