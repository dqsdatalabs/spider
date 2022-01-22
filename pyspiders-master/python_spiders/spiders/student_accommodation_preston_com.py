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
    name = 'student_accommodation_preston_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.student-accommodation-preston.com/property-category/apartments/2-beds/",
                    "https://www.student-accommodation-preston.com/property-category/apartments/3-beds/",
                    "https://www.student-accommodation-preston.com/property-category/apartments/4-beds/",
                    "https://www.student-accommodation-preston.com/property-category/apartments/5-beds-apartments/",
                    "https://www.student-accommodation-preston.com/property-category/apartments/6-beds-apartments/"
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.student-accommodation-preston.com/property-category/houses/3-bed/",
                    "https://www.student-accommodation-preston.com/property-category/houses/4-bed/",
                    "https://www.student-accommodation-preston.com/property-category/houses/5-beds/",
                    "https://www.student-accommodation-preston.com/property-category/houses/6-beds/",
                    "https://www.student-accommodation-preston.com/property-category/houses/8-beds/"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.student-accommodation-preston.com/property-category/apartments/studio-apartments/",
                ],
                "property_type": "studio"
            },
            {
                "url": [
                    "https://www.student-accommodation-preston.com/property-category/apartments/en-suite-rooms/"
                ],
                "property_type": "room"
            },
            {
                "url": [
                    "https://www.student-accommodation-preston.com/property-category/halls/boatmans-court/",
                    "https://www.student-accommodation-preston.com/property-category/halls/bowran-court/",
                    "https://www.student-accommodation-preston.com/property-category/halls/ladywell-halls/",
                    "https://www.student-accommodation-preston.com/property-category/halls/malthouse-court/",
                    "https://www.student-accommodation-preston.com/property-category/halls/oxheys-court/",
                    "https://www.student-accommodation-preston.com/property-category/halls/tulketh-road/"
                ],
                "property_type": "student_apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'View property')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Student_Accommodation_Preston_PySpider_united_kingdom")

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("=")[-1])
        
        description = " ".join(response.xpath("//div[contains(@class,'entry-content')]/p//text()").getall())
        if description:
            desc = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", desc)
        
        room_count = response.xpath("//a[img[contains(@src,'bed')]]/parent::div/span/text()").get()
        if response.meta.get('property_type') == "studio" or response.meta.get('property_type') == "room":
            item_loader.add_value("room_count", "1")
        elif room_count:
            item_loader.add_value("room_count", room_count)
        elif "bedroom" in description.lower():
            room_count = description.lower().split("bedroom")[0].strip().split(" ")[-1]
            if "four" in room_count: item_loader.add_value("room_count", "4")
             
        address = response.xpath("//h1/text()").get()
        if address:
            if "*" in address:
                address = address.split("*")[-1].strip()
            if "– New" in address:
                address = address.split("– New")[0].strip()
                
            item_loader.add_value("address", address.strip())
            zipcode = ""
            if not address.split(" ")[-2].isalpha():
                zipcode = f"{address.split(' ')[-2]} {address.split(' ')[-1]}"
                item_loader.add_value("zipcode", zipcode)
                
                city = address.split(zipcode)[0].strip().strip(",")
                if city.count(",") >0:
                    item_loader.add_value("city", city.split(",")[-1].strip())
                elif "(" in city:
                    city = city.split("(")[0].strip().split(" ")
                    item_loader.add_value("city", f"{city[-2]} {city[-1]}")
                else: item_loader.add_value("city", city)

        rent = response.xpath("//p/text()[contains(.,'Rent')]").get()
        if rent:
            rent = rent.split("£")[1].split(" ")[0]
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "GBP")
        
        import dateparser
        available_date = response.xpath("substring-after(//p/text()[contains(.,'Start')],':')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//a[img[contains(@src,'parking')]]//@src").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//a[img[contains(@src,'-furnished')]]//@src").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        washing_machine = response.xpath("//a[img[contains(@src,'washer')]]//@src").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='property-images']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Kexgill Student Accommodation")
        item_loader.add_value("landlord_phone", "01772 252389")
        item_loader.add_value("landlord_email", "preston@kexgill.com")
        
        yield item_loader.load_item()