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
    name = 'miltonstone_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.miltonstone.com/?ct_beds_from=&ct_beds_to=&ct_ct_status=to-rent&ct_property_type=flat&ct_price_from=0&ct_price_to=15000&q=&search-listings=true", "property_type": "apartment"},
	        {"url": "http://www.miltonstone.com/?ct_beds_from=&ct_beds_to=&ct_ct_status=to-rent&ct_property_type=house&ct_price_from=0&ct_price_to=15000&q=&search-listings=true", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for url in response.xpath("//div[@id='listings-results']/article//h4/a/@href").extract():
            yield Request(url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//nav[contains(@class,'content-nav')]/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Miltonstone_PySpider_"+ self.country)
        item_loader.add_xpath("title", "//header/h2/text()")
        
        address = "+".join(response.xpath("//header[@class='listing-location']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip().strip("+"))
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().split("+")[-1]
            item_loader.add_value("address", address.replace("+",""))
            item_loader.add_value("city", city.replace(",",""))
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.split("/")[0].split("£")[1].replace(",","").strip()
            item_loader.add_value("rent", str(int(price)*4))
        
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//span[contains(@class,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom = response.xpath("//ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split(" ")[0])
        
        desc = " ".join(response.xpath("//div[contains(@class,'post-content')]//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        lat_lng = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if lat_lng:
            lat = lat_lng.split("LatLng(")[1].split(",")[0].strip()
            lng = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        
        images = [ x for x in response.xpath("//figure[contains(@id,'image')]//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//span[contains(@class,'plan')]/following-sibling::a/@href[not(contains(.,'java'))]").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//ul/li[contains(.,'EPC')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(" ")[-1])
        
        terrace = response.xpath("//ul/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("//div[contains(@class,'post-content')]//text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name", "MILTON STONE")
        item_loader.add_value("landlord_phone", "0207 938 2311")
        item_loader.add_value("landlord_email", "info@miltonstone.com")
        
        
        yield item_loader.load_item()