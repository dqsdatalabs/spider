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
    name = 'letsmovepropertymanagement_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.letsmovepropertymanagement.co.uk/properties/for-rent"]
    def parse(self, response):

        base_url = "https://www.letsmovepropertymanagement.co.uk/search/json?lat=&lng=&distance=&type=let&prop_type={}&minprice=&maxprice=&minbeds=&maxbeds=&sold_properties=hide"
        token = response.xpath("//meta[@name='csrf-token']/@content").get()

        headers = {
            "X-CSRF-TOKEN": token,
            "X-Requested-With": "XMLHttpRequest",
            "X-XSRF-TOKEN": "eyJpdiI6Ik5nQWpSMWdaSkV5cWdmV1lXZFNYMFE9PSIsInZhbHVlIjoiMGYxemRYZjNrbDR2dVNyeEZlVjFqZDd1c01NS1FlajFKK1VPdUtxaHAySWJBaVwveUNveFU3WldpZlJtU09Yd2lKRTdLWXppckxhUndCSFBEVTVpTUdnPT0iLCJtYWMiOiJlM2FjMTZlZTYzYThiNGRkMmRlZTZlYzIyNTNlZDhjOWIxY2UzNmFhMzgyOWY0ODJmNTk2MjBkZGNiMmM5M2QyIn0=",
            "Host": "www.letsmovepropertymanagement.co.uk",
            "Connection": "keep-alive",
        }
        start_urls = [
            {
                "url" : "1",
                "property_type" : "house"
            },
            {
                "url" : "2",
                "property_type" : "house"
            },
            {
                "url" : "3",
                "property_type" : "house"
            },
            {
                "url" : "4",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            p_value = url.get("url")
            yield Request(
                url=base_url.format(p_value),
                callback=self.jump,
                headers=headers,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def jump(self, response):
        data = json.loads(response.body)
        for item in data:
            area, slug, ref = item["main_searchable_areas"], item["slug"], item["property_ref"]  
            follow_url = f"https://www.letsmovepropertymanagement.co.uk/properties/for-rent/{area}/{slug}/{ref}"
            item_loader = ListingLoader(response=response)
            item_loader.add_value("room_count", str(item["bedrooms"]))
            item_loader.add_value("bathroom_count", str(item["bathrooms"]))
            item_loader.add_value("external_id", item["property_ref"])
            item_loader.add_value("city", item["town"])
            item_loader.add_value("zipcode",item["postcode1"]+" "+item["postcode2"])
            item_loader.add_value("latitude", str(item["latitude"]))
            item_loader.add_value("longitude", str(item["longitude"]))
            # sq = item["floor_area"]
            # if sq:
            #     item_loader.add_value("square_meters", int(float(sq.replace("m2","").strip())))

            item_loader.add_value("rent", str(item["price"]))
            item_loader.add_value("currency", "GBP")
           
            yield Request(follow_url, callback=self.populate_item, meta={'item': item_loader,'property_type': response.meta.get('property_type')})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = response.meta.get("item")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
    
        item_loader.add_value("external_source", "Letsmovepropertymanagement_Co_PySpider_united_kingdom")
        title = response.xpath("//h1[@class='section-title']/text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//p[@class='houseSpecs--address']/text()[1]").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
                                   
        desc = " ".join(response.xpath("//div[@id='details']//text()").extract())
        if desc:
            item_loader.add_value("description",desc.strip())   
      
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'slider-single')]/div/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)   

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floor']//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
    
        floor = response.xpath("//div[@id='overview']//ul/li[contains(.,'Floor')]/text()[not(contains(.,'Floors'))]").extract_first()
        if floor: 
            item_loader.add_value("floor", floor.split("Floor")[0].strip().split(" ")[-1]) 
            
        terrace = response.xpath("//div[contains(@class,'featuresBox')]/ul/li/text()[contains(.,'Terrace') or contains(.,'terrace')]").extract_first()
        if terrace: 
            item_loader.add_value("terrace", True)    
            
        parking = response.xpath("//div[@id='overview']//ul/li[contains(.,'parking') or contains(.,'Parking')]//text()").extract_first()
        if parking: 
            item_loader.add_value("parking", True) 
      
        item_loader.add_value("landlord_email", "info@letsmovepropertymanagement.co.uk")
        item_loader.add_value("landlord_phone", "01952 825987")
        item_loader.add_value("landlord_name", "Lets Move Property Management")
        yield item_loader.load_item()
