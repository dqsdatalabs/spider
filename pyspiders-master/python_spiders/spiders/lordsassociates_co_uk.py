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
    name = 'lordsassociates_co_uk'   
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    def start_requests(self):

        formdata = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "216267ef-9d23-40a6-9f83-f2373a5d3167",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
                "includeSoldButton": "false",
            }
        
        yield FormRequest(
                url="https://www.lordsassociates.co.uk/api/set/results/grid",
                callback=self.parse,
                formdata=formdata,
                #meta={'property_type': url.get('property_type'), "type": url.get("type")}
            )

            
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='property-description-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
        
            formdata = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "216267ef-9d23-40a6-9f83-f2373a5d3167",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
                "includeSoldButton": "false",
                "page": str(page),
            }
            yield FormRequest(
                url="https://www.lordsassociates.co.uk/api/set/results/grid",
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        ext_id = response.url.split("property/")[1].split("/")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)

        title = response.xpath("//section[@id='description']//h2/text()").get()
        if title and ("apartment" in title.lower() or "flat" in title.lower()):
            item_loader.add_value("property_type", "apartment")
        elif title and "house" in title.lower():
             item_loader.add_value("property_type", "house")
        elif title and "studio" in title.lower():
             item_loader.add_value("property_type", "studio")
        elif title and "room" in title.lower():
             item_loader.add_value("property_type", "room")
        else:
            return

        item_loader.add_value("external_source", "Lordsassociates_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//section[@id='description']/div[@class='row']/div/h2/text()").extract_first()     
        if title:   
            item_loader.add_value("title",title.strip()) 

        rent = "".join(response.xpath("//div[contains(@class,'property-price')]/h2//text()[normalize-space()]").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
        room = response.xpath("//section[contains(@class,'featured-stats')]//ul/li[@class='FeaturedProperty__list-stats-item'][1]/span/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip())
        bathroom_count = response.xpath("//section[contains(@class,'featured-stats')]//ul/li[@class='FeaturedProperty__list-stats-item'][2]/span/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        furnished1 = response.xpath("//div[contains(@class,'property-price')]/h2/span[@class='property-price-subline']//text()[normalize-space()]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        elif furnished1:
            if "unfurnished" in furnished1.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
                
        parking = response.xpath("//div[@id='collapseOne']/div[@class='accordion-inner']/ul/li[contains(.,'Parking')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        address ="".join(response.xpath("//div[contains(@class,'property-address')]/h1/span/text()").extract())
        if address:   
            item_loader.add_value("address", address.strip())
            
        city = response.xpath("//div[contains(@class,'property-address')]/h1/span[@class='county']/text()").extract_first()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())
        else: 
            city = address.strip().split(",")[0].split(" ")[-1]
            item_loader.add_value("city",city)
            
        zipcode = response.xpath("//div[contains(@class,'property-address')]/h1/span[@class='displayPostCode']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
    
        map_coordinate = response.xpath("//section[@id='maps']/@data-cords").extract_first()
        if map_coordinate:
            latitude = map_coordinate.split('"lat": "')[1].split('",')[0]
            longitude = map_coordinate.split('lng": "')[1].split('"')[0]
            if latitude and longitude:
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            
        desc = " ".join(response.xpath("//section[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "washing machine" in desc.lower():
                item_loader.add_value("washing_machine", True)
            if "terrace" in desc.lower():
                item_loader.add_value("terrace", True)

        images = [x for x in response.xpath("//div[@id='propertyDetailsGallery']/div[@class='item']/div[contains(@class,'owl-image')]/@data-bg").extract()]
        if images:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "01895 233 761")
        item_loader.add_value("landlord_email", "office@lordsassociates.co.uk")
        item_loader.add_value("landlord_name", "Lords Associates of London")
        yield item_loader.load_item()
