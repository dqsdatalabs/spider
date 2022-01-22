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
import re

class MySpider(Spider):
    name = 'coastal_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Coastal_Com_PySpider_australia'
    custom_settings = {
        "PROXY_ON": True,
    }
    handle_httpstatus_list = [403]
    headers = {
        ":authority": "www.coastal.com.au",
        ":method": "GET",
        ":path": "/wp-json/api/listings/all?priceRange=&landArea=&limit=12&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
        ":scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "cookie": "selectedSiteID=1; _ga=GA1.3.2127743424.1630564364; _gid=GA1.3.2084207825.1630564364; _fbp=fb.2.1630564363657.335489133; _gcl_au=1.1.1070715299.1630564364; _gat_UA-201411469-38=1; _gat_gtag_UA_120639170_1=1",
        "pagefrom": "archive",
        "referer": "https://www.coastal.com.au/properties-for-rent/",
        "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.coastal.com.au/wp-json/api/listings/all?priceRange=&category=Unit%2CApartment%2CTerrace&landArea=&limit=1200&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.coastal.com.au/wp-json/api/listings/all?priceRange=&category=House%2CDuplexsemi-detached%2CTownhouse%2CVilla&landArea=&limit=1200&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.coastal.com.au/wp-json/api/listings/all?priceRange=&category=Studio&landArea=&limit=1200&type=rental&status=current&address=&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "studio"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)

        if "results" in data:
            for item in data["results"]:
                status = item["propertyStatus"]
                if status and "leased" in status.lower():
                    continue
                follow_url = item["slug"]
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item = response.meta.get('item')
        item_loader.add_value("external_source", "Coastal_Com_PySpider_australia")         
        title = response.xpath("//title/text()").get()
        if title: 
            item_loader.add_value("title",title.split(" - ")[0])
        item_loader.add_value("external_id", str(item["uniqueID"]))
        if "Address available" not in item["title"]:
            item_loader.add_value("address",item["title"]+", "+item["address"]["suburb"]+", "+item["address"]["postcode"])
            item_loader.add_value("zipcode", f"QLD {item['address']['postcode']}")
            item_loader.add_value("city", item["address"]["suburb"])
        else:
            item_loader.add_value("address",title.split(" - ")[0])
        item_loader.add_value("bathroom_count",item["propertyBath"])
        item_loader.add_value("room_count",item["propertyBed"])
        item_loader.add_value("landlord_name", item["agent"]["name"])
        item_loader.add_value("landlord_phone", item["agent"]["phone"])
        item_loader.add_value("landlord_email", "info@coastal.com.au")
        
        rent = item["propertyPricing"]["value"]
        if rent:
            rent = rent.split("$")[-1].lower().split("p")[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(re.sub(r'\D', '', rent))) * 4)
        item_loader.add_value("currency", 'AUD')

        available_date = item["propertyPricing"]["availabilty"]
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
  
        balcony = response.xpath("//div[contains(@class,'listing-description')]/p//text()[contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True) 
    
        coords = item["propertyCoords"]
        if coords:
            item_loader.add_value("latitude", coords.split(",")[0].strip())
            item_loader.add_value("longitude", coords.split(",")[1].strip())
        parking = item["propertyParking"]
        if parking:
            item_loader.add_value("parking", True) if parking != 0 else item_loader.add_value("parking", False)
 
        description = " ".join(response.xpath("//div[contains(@class,'listing-description')]/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("substring-before(//div[contains(@class,'listing-description')]/p//text()[contains(.,'sqm') and not(contains(.,' land'))],'sqm')").get() 
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip().split(" ")[-1])
        floor = response.xpath("substring-before(//div[contains(@class,'listing-description')]/p//text()[contains(.,'Floor ')],'Floor ')").get() 
        if floor:
            item_loader.add_value("floor", floor.strip().split(" ")[-1])
        images = [x for x in item["propertyImage"]["listImg"]]
        if images:
            item_loader.add_value("images", images)
        
        yield item_loader.load_item()