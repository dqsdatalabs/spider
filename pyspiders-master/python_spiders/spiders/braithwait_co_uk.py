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
    name = 'braithwait_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Braithwait_Co_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.braithwait.co.uk/property/?wppf_search=to-rent&wppf_property_type=apartment&wppf_radius=10&wppf_orderby=latest&wppf_view=grid&wppf_lat=0&wppf_lng=0&wppf_records=12", "property_type": "apartment"},
            {"url": "https://www.braithwait.co.uk/property/?wppf_search=to-rent&wppf_property_type=flat&wppf_radius=10&wppf_orderby=latest&wppf_view=grid&wppf_lat=0&wppf_lng=0&wppf_records=12", "property_type": "apartment"},
	        {"url": "https://www.braithwait.co.uk/property/?wppf_search=to-rent&wppf_property_type=house&wppf_radius=10&wppf_orderby=latest&wppf_view=grid&wppf_lat=0&wppf_lng=0&wppf_records=12", "property_type": "house"},
            {"url": "https://www.braithwait.co.uk/property/?wppf_search=to-rent&wppf_property_type=studio&wppf_radius=10&wppf_orderby=latest&wppf_view=grid&wppf_lat=0&wppf_lng=0&wppf_records=12", "property_type": "studio"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for url in response.xpath("//div[@class='wppf_grid_wrapper']/div[not(contains(.,'Let Agreed'))]//h4/a/@href").extract():
            yield Request(url, callback=self.populate_item, meta={"property_type":property_type})
        
        pagination = response.xpath("//div[@class='nav-links']/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        
        address = "".join(response.xpath(
            "//h1//text()"
            ).getall())
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].strip()
            if zipcode.replace(" ","").isalpha():
                item_loader.add_value("city", zipcode)
            else:
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", address.split(",")[-1].strip())
        
        room_count = "".join(response.xpath(
            "//div[i[@class='fas fa-bed']]/h6/text()"
            ).getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0])
        
        bathroom_count = "".join(response.xpath("//div[i[@class='fas fa-bath']]/h6/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0])
        
        rent = response.xpath("//div[@class='bw-column']//div[contains(.,'£')]/text()").get()
        if rent:
            price = rent.split("£")[1].split(" ")[0].strip().replace(",","")
            item_loader.add_value("rent" , price)
            
        item_loader.add_value("currency", "GBP")
        
        desc = "".join(response.xpath("//div[@class='bw-section'][1]//div//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        square_meters = response.xpath("//div[contains(@class,'bw-prop-cta-option')]//h6[contains(@class,'title is-4 bw-text-white')]//text()[contains(.,'SQ')]").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("SQ")[0])
        # if "sq ft" in desc.lower().replace(".","").replace("/"," "):
        #     desc = desc.lower().replace(".","").replace(",","").replace("/"," ")
        #     square_meters = desc.split("sq ft")[0].strip().split(" ")[-1]
        #     item_loader.add_value("square_meters", sqm)
            
        # if "dishwasher" in desc.lower():
        #     item_loader.add_value("dishwasher", True)
        # if "terrace" in desc.lower():
        #     item_loader.add_value("terrace", True)
        # if "balcony" in desc.lower():
        #     item_loader.add_value("balcony", True)
        # if "parking" in desc.lower():
        #     item_loader.add_value("parking", True)
        # if "washing machine" in desc.lower():
        #     item_loader.add_value("washing_machine", True)
        # if "furnished" in desc.lower():
        #     item_loader.add_value("furnished", True)
        # if "elevator" in desc.lower():
        #     item_loader.add_value("elevator", True)        
        
        images = [x for x in response.xpath("//div[contains(@class,'bw-section bw-prop-gallery')]//div[@class='bw-gallery-option']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//div/a[.='FLOORPLAN']/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0]
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        item_loader.add_value("landlord_name", "BRAITHWAIT")
        item_loader.add_value("landlord_phone", "0207 289 8889")
        item_loader.add_value("landlord_email", "lettings@braithwait.co.uk")
        
        yield item_loader.load_item()