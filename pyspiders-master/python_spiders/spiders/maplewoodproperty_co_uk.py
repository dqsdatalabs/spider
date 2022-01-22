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
    name = 'maplewoodproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    external_source = "Maplewoodproperty_Co_PySpider_united_kingdom"

    def start_requests(self):
        start_urls = [
            {"url": "https://www.zoopla.co.uk/to-rent/flats/north-west-england/?page_size=25&price_frequency=per_month&view_type=list&q=NW%203&radius=0&results_sort=newest_listings&search_source=facets", "property_type": "apartment"},
	        {"url": "https://www.zoopla.co.uk/to-rent/houses/north-west-england/?include_shared_accommodation=false&page_size=25&price_frequency=per_month&property_sub_type=detached&property_sub_type=terraced&property_sub_type=detached_bungalow&property_sub_type=terraced_bungalow&property_sub_type=semi_detached&property_sub_type=bungalow&property_sub_type=semi_detached_bungalow&property_sub_type=cottage&view_type=list&q=NW%203&radius=0&results_sort=newest_listings&search_source=facets", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        prop_type = response.meta.get("property_type")

        for url in response.xpath("//a[@data-testid='listing-details-link']/@href").getall():
            follow_url = "https://www.zoopla.co.uk"+url
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop_type})
        
        next = response.xpath("//a[contains(text(),'Next')]/@href").get()
        if next:
            url = "https://www.zoopla.co.uk" + next
            yield Request(url, callback=self.parse,meta={"property_type":prop_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = "".join(response.xpath("//li[@class='ui-property-indicators__item']/span/text()").extract())
        if "let" in rented.lower(): 
            return
        prop_type= response.xpath("//h1[contains(@class,'ui-property-summary__title')]/text()").extract_first()        
        if prop_type and "Studio" in prop_type: 
            item_loader.add_value("property_type", "studio")
        else: 
            item_loader.add_value("property_type", response.meta.get('property_type'))
        if "rent" not in response.url:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("details/")[-1])
        item_loader.add_value("external_source", self.external_source)
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@class,'Address')]//text()").extract_first()
        if address:
            city_zipcode = address.split(",")[-1].strip()
            city = city_zipcode.split(" ")[0]
            zipcode = city_zipcode.split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[contains(@class,'Pricing')]//text()").get()
        if rent:
            rent = rent.split("£")[-1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
   
        description = " ".join(response.xpath("//div[contains(@class,'Description')]//span//text()").extract())
        if description:
            item_loader.add_value("description", description.strip())
   
        room_count = response.xpath("//span[contains(@data-testid,'bed')]//text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        bathroom_count = response.xpath("//span[contains(@data-testid,'bath')]//text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])

        square_meters = response.xpath("//span[contains(@data-testid,'floorarea')]//text()").extract_first()
        if square_meters:
            sqm = str(int(float(square_meters.split("sq")[0].replace(",","").strip()) * 0.09290304))
            item_loader.add_value("square_meters", sqm)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        parking = response.xpath("//li[contains(.,'Parking ') or contains(.,'parking ')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking",True)
    
        images = [x for x in response.xpath("//div[contains(@class,'Gallery')]//@src").extract()]
        if images:
            item_loader.add_value("images", images)
                 
        floor_images = [x for x in response.xpath("//ul[@class='dp-floorplan-assets__no-js-links']/li/a/@href").extract()]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)  

        latitude_longitude = response.xpath("//script[contains(.,'GeoCoordinates')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split('}')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)    

        item_loader.add_value("landlord_name", "Maplewood Property")
        item_loader.add_value("landlord_phone", "020 8128 0069")
        
        yield item_loader.load_item()