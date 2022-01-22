# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'universityhousing_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en' 


    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.universityhousing.nl/residential-listings/rent/type-apartment",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.universityhousing.nl/residential-listings/rent/type-house",
                "property_type" : "house"
            },
            
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='sys-property-link multi-photo']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})
        
        next_page = response.xpath("//a[contains(@class,'next-page')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Universityhousing_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//div[@class='object_title']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        
        external_id = response.xpath("//td[.='Reference number']/parent::*/td[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        desc = "".join(response.xpath("//div[@class='description textblock']/div/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        if desc:
            item_loader.add_value("description", desc)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine",True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        
        if ("no pets" in desc.lower()) or ("pets are not allowed" in desc.lower()):
            item_loader.add_value("pets_allowed", False)
        
        
        if title:
            city = title.strip().split(",")[1].strip().split(" ")[-1]
            if city:
                item_loader.add_value("city", city)
            zipcode = title.strip().split(",")[1].strip().split(" ")[0] + " " + title.strip().split(",")[1].strip().split(" ")[1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        square_meters = response.xpath("//td[.='Liveable area']/parent::*/td[2]/text()").get()
        if square_meters and square_meters != "-":
            try:
                square_meters = square_meters.split(" ")[0]
            except:
                pass
            
        item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("//td[.='Number of rooms']/parent::*/td[2]/text()").get()
        if room_count:
            try:
                room_count = room_count.split(" ")[0]
            except:
                pass
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom=response.xpath("//td[contains(.,'bathroom')]/parent::*/td[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split("(")[0])
        
        available_date = response.xpath("//td[.='Obtainable from' or .='Offered since' ]/following-sibling::td/text()[.!='By consultation']").get()
        if available_date and available_date.isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        images = [x for x in response.xpath("//div[@id='object-photos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        

        price = response.xpath("//div[@class='block price clearfix']/span/text()").get()
        if price and price != "-": 
            price = price.split("/")[0].replace("€","").replace(",","").split(".")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//td[.='Deposit']/parent::*/td[2]/text()").get()
        if deposit and deposit != "-" :
            item_loader.add_value("deposit", deposit.replace("€","").replace(",","").split(".")[0])
        

        furnished = response.xpath("//td[.='Furnishing']/parent::*/td[2]/text()").get()
        if furnished:
            if furnished.lower() == "yes" or furnished.lower() == "Furnished":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
    
        utilities = response.xpath("//td[.='Service costs']/parent::*/td[2]/text()").get()
        if utilities:
            try:
                utilities = utilities.split(" ")[0].replace("€","").replace(",","")
            except:
                pass
            item_loader.add_value("utilities", utilities)
        

        floor = response.xpath("//td[.='Number of floors']/parent::*/td[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        latitude_longitude=response.xpath("//script[contains(.,'map')]/text()").get()
        if latitude_longitude:
            longitude=latitude_longitude.split("center: [")[1].split(",")[0]
            latitude=latitude_longitude.split("center: [")[1].split(",")[1].split("]")[0].strip()
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)
            address=latitude_longitude.split("location:")[1].split(",")[0].replace('"','')
            if address:
                item_loader.add_value("address", address)
        item_loader.add_value("landlord_name","University Housing")
        item_loader.add_value("landlord_phone","31 88 1703 000")
        item_loader.add_value("landlord_email","mynewplace@universityhousing.nl")	
     
        yield item_loader.load_item()
