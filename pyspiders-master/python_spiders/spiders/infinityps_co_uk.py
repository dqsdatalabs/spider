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
    name = 'infinityps_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "0"
            },
            {
                "property_type" : "house",
                "type" : "1"
            },
            {
                "property_type" : "apartment",
                "type" : "4"
            },
            {
                "property_type" : "house",
                "type" : "5"
            },
            {
                "property_type" : "house",
                "type" : "6"
            },
            {
                "property_type" : "house",
                "type" : "9"
            },
            {
                "property_type" : "house",
                "type" : "10"
            },
            {
                "property_type" : "house",
                "type" : "11"
            },
        ]
        for item in start_urls:
            formdata = {
                "chkShowCTC": "false",
                "ddlArea": "",
                "ddlBedrooms": "0",
                "ddlMaxPrice": "100000000",
                "ddlMinPrice": "0",
                "ddlProperyType": item["type"],
                "lblLocation": "",
                "lblPostCode": "",
                "page": "1",
                "rdbBuyOrRent": "Rent",
                "selLowHigh": "false",
            }
            yield FormRequest(
                "https://infinityps.co.uk/Umbraco/Surface/Property/searchProperty",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type" : item["type"],
                })
    

    def parse(self, response):
        page = response.meta.get("page", 2)
        data = json.loads(response.body)
        data2 = json.loads(data)
        try:
            for item in data2["property"]:
                follow_url = f"https://infinityps.co.uk/property-detail/{item['id']}/"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "items":item})
        except:
            pass
        if data2["pager"][0]["page"] < data2["pager"][0]["pageCount"]:
            p_type = response.meta["type"]
            formdata = {
                "chkShowCTC": "false",
                "ddlArea": "",
                "ddlBedrooms": "0",
                "ddlMaxPrice": "100000000",
                "ddlMinPrice": "0",
                "ddlProperyType": p_type,
                "lblLocation": "",
                "lblPostCode": "",
                "page": str(page),
                "rdbBuyOrRent": "Rent",
                "selLowHigh": "false",
            }
            yield FormRequest(
                "https://infinityps.co.uk/Umbraco/Surface/Property/searchProperty",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":response.meta["property_type"],
                    "type" : p_type,
                    "page" : page+1,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Infinityps_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("property_type", response.meta.get('property_type'))

        items = response.meta.get('items')
        item_loader.add_value("zipcode", items["postcode"])
        
        item_loader.add_value("title", items["useAddress"]+", "+items["postcode"])
        item_loader.add_value("address", items["useAddress"]+", "+items["postcode"])
        item_loader.add_value("latitude", items["latitude"])
        item_loader.add_value("longitude", items["longitude"])
        item_loader.add_value("rent_string", str(items["price"]).replace(",",""))
        
        item_loader.add_value("description", items["summaryDescription"].replace("<br/>", "").strip())
        
        room_count = items["bedrooms"]
        if room_count != "0":
            item_loader.add_value("room_count", room_count)
            
        bathroom_count = items["bathrooms"]
        if bathroom_count != "0":
            item_loader.add_value("bathroom_count", bathroom_count)
        
        city = items["city"]
        town = items["town"]
        if city:
            item_loader.add_value("city", city)
        elif town:
            item_loader.add_value("city", town)
            
        furnished = items["featured"]
        if furnished:
            item_loader.add_value("furnished", True)

        garages = items["garages"]
        parking = items["parkingSpaces"]
        if garages or parking:
            if garages and garages !='0' or parking and parking !='0':
                item_loader.add_value("parking", True)
        
        available_date = items["dateInstructed"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        item_loader.add_value("landlord_name", "INFINITY PROPERTY SOLUTIONS")
        item_loader.add_value("landlord_phone", items["EATel"])
        item_loader.add_value("landlord_email", "info@infinityps.co.uk")
        ext_id = response.url.split("/")[-2]
        image_url = f"https://infinityps.co.uk/Umbraco/surface/property/GetPropertyDetails?pId={ext_id}"
        if image_url:
            yield Request(
                image_url,
                callback=self.get_image,
                meta={
                    "item_loader" : item_loader 
                }
            )
        else:
            yield item_loader.load_item()

    def get_image(self, response):
        item_loader = response.meta.get("item_loader")
        data = json.loads(response.body)
        data2 = json.loads(data)
        images = [x["picture_Text"]+"&width=1860&height=1015" for x in data2["picture"] if "/ImageResizeHandler.do" in x["picture_Text"]]
        if images:
            item_loader.add_value("images", images)  
            
        yield item_loader.load_item()
