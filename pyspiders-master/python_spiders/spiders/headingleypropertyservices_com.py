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
    name = 'headingleypropertyservices_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Apartments / Flats",
            },
            {
                "property_type" : "house",
                "type" : "house",
            },
        ]
        for item in start_urls:
            formdata = {
                "data[property-type][slug]": "property-type",
                "data[property-type][baseSlug]": "property_type",
                "data[property-type][key]": "property-type",
                "data[property-type][units]": "",
                "data[property-type][compare]": "=",
                "data[property-type][values][0][name]": item["type"],
                "data[property-type][values][0][value]": item["property_type"],
                "page": "1",
                "limit": "999999",
                "sortBy": "newest",
                "currency": "any",
            }
            api_url = "https://headingleypropertyservices.com/wp-json/myhome/v1/estates?currency=any"
            yield FormRequest(
                url=api_url,
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data["results"]:
            follow_url = item["link"]
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item = response.meta["item"]
        item_loader.add_value("external_source", "Headingleypropertyservices_PySpider_united_kingdom")

        item_loader.add_value("external_id",str(item["id"]) )
        item_loader.add_value("title",str(item["name"]) )

        if "attributes" in item:
            for data in item["attributes"]:
                if "Bedrooms" in data["name"]:
                    if data["values"]:
                        item_loader.add_value("room_count", data["values"][0]["value"])
                if "Bathrooms" in data["name"]:
                    if data["values"]:
                        item_loader.add_value("bathroom_count", data["values"][0]["value"])
                if "Letting Type" in data["name"]:
                    if data["values"]:
                        available_date = data["values"][0]["value"]
                        date_parsed = dateparser.parse(available_date.replace("start","").strip(), date_formats=["%d/%m/%Y"], languages=['en'])
                        if date_parsed:
                            date2 = date_parsed.strftime("%Y-%m-%d")
                            item_loader.add_value("available_date", date2)
                 
        if "address" in item:
            address = item["address"]
            if address:
                item_loader.add_value("address", address)
                if len(address.split(",")) > 2:
                    city = address.split(",")[-2].strip()
                    if city.isalpha():
                        item_loader.add_value("city",city)
            else:
                item_loader.add_value("address", str(item["name"]))
            
            
        if "price" in item:
            rent = item["price"][0]["price"]
            item_loader.add_value("rent_string", rent)

        description = " ".join(response.xpath("//div[contains(@class,'mh-estate__section--description')]/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        images = [x for x in response.xpath("//div[contains(@class,'swiper-container--single')]/div[contains(@class,'mh-popup-group')]/div[@class='swiper-slide']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Headingley Property Services")
        item_loader.add_value("landlord_phone", "0113 275 7668")
        item_loader.add_value("landlord_email", "info@headingleypropertyservices.com")
        yield item_loader.load_item()
