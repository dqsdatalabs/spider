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
import dateparser

class MySpider(Spider):
    name = 'lskk_com_au_disabled'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lsre.com.au/kingsford/wp-json/api/listings/all?priceRange=&category=Apartment&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lsre.com.au/kingsford/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://lsre.com.au/kingsford/wp-json/api/listings/all?priceRange=&category=Studio&limit=18&type=rental&status=current&address=&paged=1",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        if data["status"].upper() == 'SUCCESS':
            seen = True
            for item in data["results"]:           
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"],"item":item})

        if page == 2 or seen: 
            yield Request(response.url.split('&paged=')[0] + f"&paged={page}", callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item = response.meta.get('item')
        item_loader.add_value("external_source", "Lskk_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", item["uniqueID"])

        room= item["propertyBed"]
        if room!="0":
            item_loader.add_value("room_count", room)
        else:
            if response.meta.get('property_type')=="studio":
                item_loader.add_value("room_count", "1")

        rent = item["propertyPricing"]["value"]
        if rent:
            price =  rent.replace("From","").strip().split(" ")[0].split("$")[1].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","USD")

        available_date=item["propertyPricing"]["availabilty"]
        if available_date:
            date2 =  " ".join(available_date.split(" ")[1:]).replace("from","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        latlng = item["propertyCoords"]
        if latlng:
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        images = [ x for x in item["propertyImage"]["listImg"]]
        if images:
            item_loader.add_value("images", images) 

        floor_plan_images =  [ x for x in response.xpath("//div[@class='floorplan-wrapper']/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = item["propertyParking"]   
        if parking:
            (item_loader.add_value("parking", True) if parking !=0 else item_loader.add_value("parking", False))
        else:
            parking = "".join(response.xpath("//li[@class='balcony']/text()").extract())
            if parking:
                item_loader.add_value("parking", True)


        item_loader.add_value("bathroom_count", item["propertyBath"])
        item_loader.add_value("address", "{} {}".format(item["title"],item["address"]["suburb"]))
        item_loader.add_value("city", "{}".format(item["address"]["suburb"]))

        balcony = "".join(response.xpath("//li[@class='garage']/text()").extract())
        if balcony:
           item_loader.add_value("balcony", True)

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@id='author-info']/h3/a/text())")
        item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='author-info']//div[@class='meta-content agent-mobile']/a/span/text())")
        item_loader.add_xpath("landlord_email", "normalize-space(//div[@class='author-info']//div[@class='meta-content agent-email']/a/span/text())") 

        

        yield item_loader.load_item()