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

class MySpider(Spider):
    name = 'cjpropertyservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'   
   
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=apartment&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=flat&wppf_search=to-rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=detached-house&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=end-terraced-house&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=mid-terraced-house&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=semi-detached-bungalow&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=semi-detached-house&wppf_search=to-rent",
                    "https://www.cjpropertyservices.co.uk/property-to-let/?wppf_radius=10&si_radius=10&wppf_radius=10&si_bedrooms=1%3B5&wppf_min_bedrooms=1&wppf_max_bedrooms=5&si_rent=100%3B2500&wppf_min_rent=100&wppf_max_rent=2500&wppf_property_type=town-house&wppf_search=to-rent",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'wppf_list')]//a[contains(.,'More Details')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cjpropertyservices_Co_PySpider_united_kingdom")    
        item_loader.add_xpath("title","//div[@class='wppf_property_title']/h1/text()")   
    
        address ="".join(response.xpath("//p[strong[.='Location:']]/text()").extract())
        if address:
            item_loader.add_value("address",address.strip())      
            item_loader.add_value("city",address.split(",")[-2].strip())      
            item_loader.add_value("zipcode",address.split(",")[-1].strip())      
        
        item_loader.add_xpath("bathroom_count", "//div[strong[.='Bathrooms:']]/text()[.!='0']")
        item_loader.add_xpath("room_count", "//div[strong[.='Bedrooms:']]/text()[.!='0']")
  
        rent = response.xpath("//div[@class='wppf_price']/text()").extract_first()
        if rent:   
            item_loader.add_value("rent_string",rent) 
        available_date = response.xpath("//div[@class='wppf_available']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1].strip(), date_formats=["%d.%m.%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        floor = response.xpath("//ul/li[contains(.,'floor ')]//text()").extract_first()    
        if floor:
            item_loader.add_value("floor", floor.split("floor ")[0].strip().split(" ")[-1])

        parking = response.xpath("//ul/li[contains(.,'Parking') or contains(.,'Garage') or contains(.,'GARAGE') or contains(.,'parking') or contains(.,'garage')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//ul/li[contains(.,'terrace')]//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
       
        furnished = response.xpath("//ul/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()  
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'wppf_propety_full_description')]/div/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        latlng = response.xpath("//script/text()[contains(.,'wppf_LatLng = new google.maps.LatLng(')]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('wppf_LatLng = new google.maps.LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latlng.split('wppf_LatLng = new google.maps.LatLng(')[1].split(',')[1].split(')')[0].strip()) 
        images = [response.urljoin(x) for x in response.xpath("//div[@id='wppf_slideshow']/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
     
        item_loader.add_value("landlord_name", "CJ Property")
        item_loader.add_value("landlord_phone", "01482 645270")
        item_loader.add_value("landlord_email", "info@cjpropertyservices.co.uk")

        yield item_loader.load_item()