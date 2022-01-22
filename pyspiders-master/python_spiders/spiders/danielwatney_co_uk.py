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
    name = 'danielwatney_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.danielwatney.co.uk/residential-lettings/apartments-available-to-rent-in-london/page-1", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.danielwatney.co.uk/residential-lettings/houses-available-to-rent-in-london/page-1", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[contains(@class,'item')]//div[contains(@class,'item-image')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type")})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page-{page-1}", f"page-{page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
                

        item_loader.add_value("external_source", "Danielwatney_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = response.xpath("//span[contains(@class,'property-title')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@class,'property-sub-title')]//text()").get()
        if address:
            city = address.split(",")[0]
            zipcode = address.split(",")[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//span[contains(@class,'price-qualifier')]//text()").get()
        if rent:
            rent = rent.strip().replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property-content__right')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'bed')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'bath')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'slides')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        square_meters="".join(response.xpath("//li//text()[contains(.,'sq ft')]").extract())
        if square_meters: 
            squ=square_meters.split("sq")[0].replace(" ","")
            if squ:
                squ=str(int(float(squ) * 0.09290304))
                if squ:
                    item_loader.add_value("square_meters",squ)

        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        swimming_pool = response.xpath("//li[contains(.,'swimming pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        latitude_longitude = response.xpath("//script[contains(.,'lat:')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip() 
            longitude = latitude_longitude.split('lng:')[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "DANIEL WATNEY")
        item_loader.add_value("landlord_phone", "020 3077 3400")
        item_loader.add_value("landlord_email", "info@danielwatney.co.uk")
        yield item_loader.load_item()