# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider):
    name = 'lyons_estates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'  
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lyons-estates.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=23&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword="
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lyons-estates.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=19&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword=",
                    "https://lyons-estates.co.uk/property-search/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=10&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&address_keyword="
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div/a[@class='w-100']/@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//ul[@class='page-numbers']/li/a[.='→']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lyonsestates_Co_PySpider_united_kingdom")
        rented = response.xpath("//li[@class='availability']/text()[.=' Let Agreed']").get()
        if rented:
            return
        external_id = response.xpath("//li[@class='reference-number']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        item_loader.add_xpath("title", "//h1/text()")
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
       
        item_loader.add_xpath("rent_string", "//div[@class='price p-3']/h4/text()")
        desc = " ".join(response.xpath("//div[@class='col-12 summary']//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "//li[@class='bedrooms']/text()")
        item_loader.add_xpath("bathroom_count", "//li[@class='bathrooms']/text()")
        
        balcony = response.xpath("//li[contains(.,'balcony' or contains(.,'Balcony'))]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        latitude_longitude = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if latitude_longitude:     
            item_loader.add_value("latitude", latitude_longitude.split("google.maps.LatLng(")[1].split(",")[0])
            item_loader.add_value("longitude", latitude_longitude.split("google.maps.LatLng(")[1].split(",")[1].split(")")[0])
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/ul[@class='slides']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)         
     
        item_loader.add_value("landlord_name", "LYONS ESTATES")
        item_loader.add_value("landlord_phone", "0151 294 3232")
        item_loader.add_value("landlord_email", "info@lyons-estates.co.uk")
   
        yield item_loader.load_item()