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
    name = 'metropole_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'      
    def start_requests(self):
        start_url = "https://propertymanagement.metropole.com.au/rental-property-search/"
        yield Request(start_url, callback=self.jump)
    
    def jump(self, response):

        for item in response.xpath("//a[contains(@href,'rental-property-search-')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.start_requests2)

    # 1. FOLLOWING
    def start_requests2(self, response):

        queries = [
            {
                "query" : [
                    "?search=&type=Apartment&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                    "?search=&type=Unit&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                ],
                "property_type" : "apartment",
            },
            {
                "query" : [
                    "?search=&type=House&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                    "?search=&type=Townhouse&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                    "?search=&type=Villa&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                ],
                "property_type" : "house"
            },
            {
                "query" : [
                    "?search=&type=Studio&rent_min=&rent_max=&bedrooms=&bathrooms=&car_spaces=&pgn=1",
                ],
                "property_type" : "studio",
            },
        ]
        
        for item in queries:
            for query in item.get("query"):
                yield Request(response.url + query,
                            callback=self.parse,
                            meta={'property_type': item.get('property_type')})
    
    def parse(self, response):
        
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='property-item-title']/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen: 
            base_url = response.url.split('&pgn=')[0]
            yield Request(base_url + f"&pgn={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Metropole_Com_PySpider_australia")    
        item_loader.add_xpath("title", "//div[@class='row address']/div/text()")        
        item_loader.add_value("external_id",response.url.split("?oid=")[-1].split("&")[0])
        item_loader.add_xpath("address","//div[@class='row address']/div/text()")
        city = response.xpath("//div[@class='row address']/div/text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[-2].strip())
            item_loader.add_value("zipcode", city.split(",")[-1].strip())

        item_loader.add_xpath("room_count", "//div[@class='row properties']/div/i[@class='fas fa-bed']/following-sibling::text()[1]")
        item_loader.add_xpath("bathroom_count", "//div[@class='row properties']/div/i[@class='fas fa-bath']/following-sibling::text()[1]")

        rent = response.xpath("//div[@class='row properties']/div/i[@class='fas fa-tag']/following-sibling::text()[1]").get()
        if rent:
            item_loader.add_value("rent", rent.replace("$","").strip())
            item_loader.add_value("currency", "AUD")
  
        parking = response.xpath("//div[@class='row properties']/div/i[@class='fas fa-car']/following-sibling::text()[1]").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        available_date = response.xpath("//div[contains(@class,'available-info')]//b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@class='description']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='carousel']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        script_map = response.xpath("//script/text()[contains(.,'lng:')]").get()
        if script_map:
            latlng = script_map.split("{lat:")[1].split("}")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("lng:")[1].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='agent-data']/div[@class='agent-name']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-data']/div[@class='agent-phone']/text()")
        item_loader.add_value("landlord_email", "info@metropole.com.au")

        yield item_loader.load_item()