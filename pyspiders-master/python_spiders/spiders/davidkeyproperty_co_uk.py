# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'davidkeyproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'      
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.davidkeyproperty.co.uk/properties/?page=1&propind=L&orderBy=PriceSearchAmount&orderDirection=DESC&searchbymap=false&businessCategoryId=1&searchType=list&sortBy=highestPrice",
                ],
                "property_type": "apartment"
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
        
        for item in response.xpath("//div[contains(@class,'listprop')]"):
            status = item.xpath(".//div[@class='status']//@alt").get()
            room_count = item.xpath(".//span[@class='beds']/text()").get()
            bathroom_count = item.xpath(".//span[@class='bathrooms']/text()").get()
            follow_url = response.urljoin(item.xpath(".//div[contains(@class,'photo')]/a/@href").get())
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"bathroom_count":bathroom_count,"room_count":room_count})

        next_page = response.xpath("//a[@id='next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Davidkeyproperty_Co_PySpider_united_kingdom")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
        external_id = response.xpath("//div[@class='reference']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1].strip())
        address = response.xpath("//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                city_zip = address.split(",")[-1].strip()
                if city_zip:
                    zipcode = city_zip.split(" ")[-1].strip()
                    if not zipcode.isalpha():
                        item_loader.add_value("zipcode", zipcode)
                        item_loader.add_value("city", " ".join(city_zip.split(" ")[:-1]))
                    else:
                        city = city_zip.split(",")[-1]
                        item_loader.add_value("city", city)
            else:
                zipcode = " ".join(address.strip().split(" ")[-2:])
                if zipcode.replace(" ","").isalpha():
                    pass
                else:
                    item_loader.add_value("zipcode", zipcode)

        if item_loader.get_collected_values("city"):
            pass
        else:
            item_loader.add_value("city","London")
        item_loader.add_xpath("rent_string", "//div[@class='price']/span/text()")
    
        item_loader.add_xpath("bathroom_count", response.meta.get('bathroom_count'))
        

        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        if response.meta.get('room_count') and response.meta.get('room_count') != '0':
            item_loader.add_xpath("room_count", response.meta.get('room_count'))
        elif 'single room' in description.lower():
            item_loader.add_value('room_count', '1')
        lat_lng = response.xpath("//input[@name='mapmarker']/@value").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("lat='")[-1].split("'")[0])
            item_loader.add_value("longitude", lat_lng.split("lng='")[-1].split("'")[0])
         
        parking = response.xpath("//li//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        floor = response.xpath("//li//text()[contains(.,'Floor') and not(contains(.,'Floors'))]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        images = [response.urljoin(x) for x in response.xpath("//div[@id='photocontainer']//div[@class='propertyimagelist']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='hiddenfloorplan']//div[@class='propertyimagelist']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
   
        item_loader.add_value("landlord_name", "David Key Property")
        item_loader.add_value("landlord_phone", "020 7100 0754")
        item_loader.add_value("landlord_email", "Harringay@david-key.co.uk")
        yield item_loader.load_item()